package capstone.ignite;

import capstone.bean.ignite.BotBean;
import capstone.bean.ignite.InteractionBean;
import capstone.bean.stream.Interaction;
import capstone.cassandra.CassandraWriter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple4;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IgniteDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteDAO.class);
    private Ignite ignite = null;//IgniteStarter.start();

    private final IgniteCache<Long, InteractionBean> interactions;
    private final IgniteCache<String, BotBean> bots;

    private final AtomicLong atomicLong = new AtomicLong(0);

    private static CacheConfiguration<Long, InteractionBean> interactionCacheConfig() {
        CacheConfiguration<Long, InteractionBean> cfg = new CacheConfiguration<>("interactions");
        cfg.setIndexedTypes(Long.class, InteractionBean.class);
        return cfg;
    }

    private static CacheConfiguration<String, BotBean> botCacheConfig() {
        CacheConfiguration<String, BotBean> cacheConfiguration = new CacheConfiguration<>("bots");
        cacheConfiguration.setIndexedTypes(String.class, BotBean.class);
        return cacheConfiguration;
    }

    public IgniteDAO() {
        this.interactions = ignite.getOrCreateCache(interactionCacheConfig());
        this.bots = ignite.getOrCreateCache(botCacheConfig());
        new IgniteDAOJob();
    }

    public void addInteraction(final Interaction interaction) {
        try (IgniteDataStreamer<Long, InteractionBean> stmr = ignite.dataStreamer(this.interactions.getName())) {
            if (!IgniteStarter.hasServerNodes(ignite))
                return;

            final InteractionBean bean = Optional.ofNullable(interaction).map(i ->
                    new InteractionBean(i.getIp(), i.getDate(), i.getCategoryId(), i.getInteractionType())
            ).orElseThrow(RuntimeException::new);
            stmr.addData(atomicLong.getAndIncrement(), bean);
        } catch (CacheException e) {
            if (!e.getCause().getClass().isAssignableFrom(IgniteInterruptedException.class)) {
                throw e;
            }
        }
    }

    private void addBot(final Tuple4<String, Boolean, Boolean, Boolean> ip) {
        try (IgniteDataStreamer<String, BotBean> stmr = ignite.dataStreamer(this.bots.getName())) {
            if (!IgniteStarter.hasServerNodes(ignite))
                return;
            stmr.addData(ip._1(), new BotBean(ip._1(), System.currentTimeMillis()));
        } catch (CacheException e) {
            if (!e.getCause().getClass().isAssignableFrom(IgniteInterruptedException.class)) {
                throw e;
            }
        }
    }


    private final class IgniteDAOJob {

        public IgniteDAOJob() {
            createContinousQuery();
        }

        private Boolean getRatio(final List<InteractionBean> interactionsByIp) {
            final Long view = interactionsByIp.stream().filter(i -> i.getInteractionType().equalsIgnoreCase("view")).count();
            final Long click = interactionsByIp.stream().filter(i -> i.getInteractionType().equalsIgnoreCase("click")).count();
            return view == 0 || (click / view) > 3;
        }

        private Boolean getTotRequests(final List<InteractionBean> interactionsByIp) {
            return interactionsByIp.size() > 1000;
        }

        private Boolean getTotCategories(final List<InteractionBean> interactionsByIp) {
            return interactionsByIp
                    .stream()
                    .collect(Collectors.toMap(InteractionBean::getCategoryId,
                            Function.identity(),
                            (a, b) -> a,
                            HashMap::new))
                    .keySet()
                    .size() > 30;
        }

        private void createContinousQuery() {
            final Thread continous = new Thread(() -> {
                while (true) {
                    SqlQuery<Long, InteractionBean> qry1 = new SqlQuery<>(InteractionBean.class, "select * from InteractionBean");
//                            +" where " +
//                            "InteractionBean.date <= CURRENT_TIMESTAMP() AND InteractionBean.date >= (CURRENT_TIMESTAMP() - 600000)");
                    final List<Cache.Entry<Long, InteractionBean>> query = interactions.query(qry1).getAll();
                    final Map<String, List<InteractionBean>> grouppedByIp = query
                            .stream()
                            .map(Cache.Entry::getValue)
                            .filter(i -> i.getDate() <= System.currentTimeMillis() && i.getDate() >= (System.currentTimeMillis() - 600000))
                            .collect(Collectors.groupingBy(InteractionBean::getIp));
                    grouppedByIp.entrySet()
                            .stream()
                            .map(set -> Tuple4.apply(set.getKey(),
                                    getRatio(set.getValue()),
                                    getTotRequests(set.getValue()),
                                    getTotCategories(set.getValue())))

                            .filter(tuple -> tuple._2() || tuple._3() || tuple._4())
                            //.map(Tuple4::_1)
                            .forEach(IgniteDAO.this::addBot);
                }
            });
            continous.start();
            final Thread botThread = new Thread(() -> {
                while (true) {
                    SqlQuery<String, BotBean> statsQry = new SqlQuery<>(BotBean.class, "select * from BotBean");
                    final List<Cache.Entry<String, BotBean>> all = bots.query(statsQry).getAll();
                    all.forEach(elem -> CassandraWriter.process(elem.getValue()));
                    try {
                        Thread.sleep(100000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            botThread.start();
        }
    }
}
