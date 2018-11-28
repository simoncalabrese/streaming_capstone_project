package capstone.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class IgniteStarter {

    private static final String RATIO = "(" +
            "(SELECT count(*) FROM InteractionBean i1 WHERE i1.interactionType = 'view' AND i1.ip=i.ip) " +
            " / " +
            "(SELECT count(*) FROM InteractionBean i2 WHERE i2.interactionType = 'click' AND i2.ip=i.ip)" +
            ") > 3";
    private static final String TOT_REQUESTS = "(" +
            "SELECT count(*) FROM InteractionBean i3 " +
            "WHERE i3.ip=i.ip" +
            ") > 1000";

    private static final String TOT_CATEGORIES = "(" +
            "SELECT COUNT(DISTINCT i4.categoryId) " +
            "FROM InteractionBean i4 " +
            "WHERE i4.ip=i.ip" +
            ") > 3";
    static final String QUERY = "SELECT Interaction.ip,"
            + RATIO + ","
            + TOT_REQUESTS + ","
            + TOT_CATEGORIES + ","
            + " FROM Interaction i "
            + " WHERE i.date <= CURRENT_TIMESTAMP() and i.date >= (CURRENT_TIMESTAMP()-600000)";


    public static Ignite start() {
        Ignition.setClientMode(true);
        final Ignite ignite = Ignition.start("example-ignite.xml");
        ignite.services(ignite.cluster().forServers());
        return ignite;
    }

    public static boolean hasServerNodes(Ignite ignite) {
        if (ignite.cluster().forServers().nodes().isEmpty()) {
            System.err.println("Server nodes not found (start data nodes with ExampleNodeStartup class)");
            return false;
        }
        return true;
    }
}
