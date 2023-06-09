package com.czl.datalake.metadata.api.enums;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/30
 * Description:
 */
public enum LakeFormat {

    DELTA("delta","com.czl.datalake.metadata.delta.DeltaCatalog"),

    HOODIE("hudi","com.czl.datalake.metadata.hudi.HudiCatalog"),

    ICEBERG("iceberg","com.czl.datalake.metadata.iceberg.IcebergCatalog"),
    ;

    private final String type;

    private final String className;

    LakeFormat(String type, String className) {
        this.type = type;
        this.className = className;
    }

    public static String getClassName(String lakeFormat) {
        for (LakeFormat format : LakeFormat.values()) {
            if (format.getType().equals(lakeFormat)) {
                return format.getClassName();
            }
        }
        return null;
    }

    public String getType() {
        return type;
    }

    public String getClassName() {
        return className;
    }
}
