package database_project1;

class TableMetaData {
    private String tableName;
    private String columnName;
    private String columnType;
    private boolean clusteringKey;
    private String indexName;
    private String indexType;

    // Constructor
    public TableMetaData(String tableName, String columnName, String columnType, boolean clusteringKey, String indexName, String indexType) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.clusteringKey = clusteringKey;
        this.indexName = indexName;
        this.indexType = indexType;
    }

    // Getters
    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public boolean isClusteringKey() {
        return clusteringKey;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexType() {
        return indexType;
    }
}

