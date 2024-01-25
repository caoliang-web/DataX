package com.alibaba.datax.plugin.reader.dorisreader;



import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

public class PartitionDefinition implements Serializable, Comparable<PartitionDefinition> {
    private final String database;
    private final String table;
    private final String beAddress;
    private final Set<Long> tabletIds;
    private final String queryPlan;


    public PartitionDefinition(String database, String table, String beAddress, Set<Long> tabletIds, String queryPlan) {
        this.database = database;
        this.table = table;
        this.beAddress = beAddress;
        this.tabletIds = tabletIds;
        this.queryPlan = queryPlan;

    }


    public String getBeAddress() {
        return beAddress;
    }

    public Set<Long> getTabletIds() {
        return tabletIds;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getQueryPlan() {
        return queryPlan;
    }




    @Override
    public int compareTo(PartitionDefinition o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionDefinition that = (PartitionDefinition) o;
        return Objects.equals(database, that.database) && Objects.equals(table, that.table) && Objects.equals(beAddress, that.beAddress) && Objects.equals(tabletIds, that.tabletIds) && Objects.equals(queryPlan, that.queryPlan);
    }

    @Override
    public int hashCode() {
        int result = database.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + beAddress.hashCode();
        result = 31 * result + queryPlan.hashCode();
        result = 31 * result + tabletIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PartitionDefinition{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", beAddress='" + beAddress + '\'' +
                ", tabletIds=" + tabletIds +
                ", queryPlan='" + queryPlan + '\'' +
                '}';
    }
}

