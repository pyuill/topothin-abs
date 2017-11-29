/*
 * Copyright (c) 2017 Peter Yuill
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 */
package au.id.yuill.topothin.abs;

import au.id.yuill.topothin.*;

import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * An implementation of the Table interface to deal with Australian Statistical Geography Standard
 * datasets published by the Australian Bureau of Statistics.
 *
 * @version 1.0
 * @author Peter Yuill
 */
public class AbsTable implements Table {

    protected String releaseYear;
    protected String tableName;
    protected String additionalWhere;
    protected int count;

    public AbsTable(String releaseYear, String tableName, String additionalWhere) {
        this.releaseYear = releaseYear;
        this.tableName = tableName;
        this.additionalWhere = additionalWhere;
    }

    public void populateTopoCoordData(Connection conn, WKBReader reader, TopoCoordData tcd) throws Exception {
        count = 0;
        System.out.print("Load " + tableName + " ");
        Statement stmt = conn.createStatement();
        StringBuilder buf = new StringBuilder();
        buf.append("select ");
        buf.append(tableName);
        buf.append("_code");
        buf.append(releaseYear);
        buf.append(", ");
        buf.append(tableName);
        buf.append("_name");
        buf.append(releaseYear);
        buf.append(", ST_AsEWKB(geom) from ");
        buf.append(tableName);
        buf.append(releaseYear);
        buf.append(" where geom is not null");
        if (additionalWhere != null) {
            buf.append(additionalWhere);
        }
        ResultSet rs = stmt.executeQuery(buf.toString());
        while(rs.next()) {
            Row row = new Row();
            row.table = this;
            row.code = rs.getString(1);
            row.name = rs.getString(2);
            row.mp = (MultiPolygon)reader.read(rs.getBytes(3));
            tcd.addRow(row);
            count++;
            if ((count % 10) == 0) {
                System.out.print("*");
            }
        }
        System.out.println("*");
        rs.close();
        stmt.close();
    }

    public void saveThinnedGeometry(Connection conn, WKBWriter writer, TopoCoordData tcd) throws Exception {
        count = 0;
        System.out.print("Save " + tableName + " ");

        List<Row> rowList = tcd.tableMap.get(this);
        StringBuilder buf = new StringBuilder();
        buf.append("update ");
        buf.append(tableName);
        buf.append("_disp set geom = ? where ");
        buf.append(tableName);
        buf.append("_code = ?");
        PreparedStatement ps = conn.prepareStatement(buf.toString());
        for (Row row: rowList) {
            ps.setBytes(1, writer.write(row.mp));
            ps.setString(2, row.code);
            ps.execute();
            count++;
            if ((count % 10) == 0) {
                System.out.print("*");
            }
        }
        System.out.println("*");
        ps.close();
        Statement stmt = conn.createStatement();
        buf.setLength(0);
        buf.append("update ");
        buf.append(tableName);
        buf.append("_disp set geojson = ST_AsGeoJSON(geom,6,0) where geom is not null");
        stmt.execute(buf.toString());
        stmt.close();
    }

    @Override
    public String toString() {
        return tableName + releaseYear;
    }
}
