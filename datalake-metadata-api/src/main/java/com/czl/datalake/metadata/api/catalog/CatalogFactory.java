package com.czl.datalake.metadata.api.catalog;

import com.czl.datalake.metadata.api.enums.LakeFormat;
import com.czl.datalake.metadata.api.exception.MetaDataException;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/30
 * Description:
 */
public class CatalogFactory {

    public static Catalog getCatalog(String lakeFormat) {
        try {
            String className = LakeFormat.getClassName(lakeFormat);
            Class<?> aClass = Class.forName(className);
            return (Catalog) aClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
           throw new MetaDataException(e);
        }
    }
}
