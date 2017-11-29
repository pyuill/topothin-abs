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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * A specific implementation of Table to deal with state attribute of Local Government Areas (LGA).
 *
 * @version 1.0
 * @author Peter Yuill
 */
public class LgaTable extends AbsTable {

    public LgaTable(String releaseYear, String additionalWhere) {
        super(releaseYear, "lga", additionalWhere);
    }

    public void populateTopoCoordData(Connection conn, WKBReader reader, TopoCoordData tcd) throws Exception {
        count = 0;
        System.out.print("Load " + tableName + " ");
        Statement stmt = conn.createStatement();
        StringBuilder buf = new StringBuilder();
        buf.append("select ");
        buf.append("lga_code");
        buf.append(releaseYear);
        buf.append(", lga_name");
        buf.append(releaseYear);
        buf.append(", ste_code");
        buf.append(releaseYear);
        buf.append(", ST_AsEWKB(geom) from lga");
        buf.append(releaseYear);
        buf.append(" where geom is not null");
        if (additionalWhere != null) {
            buf.append(additionalWhere);
        }
        ResultSet rs = stmt.executeQuery(buf.toString());
        while(rs.next()) {
            LgaRow row = new LgaRow();
            row.table = this;
            row.code = rs.getString(1);
            row.name = rs.getString(2);
            row.stateCode = rs.getString(3);
            row.mp = (MultiPolygon)reader.read(rs.getBytes(4));
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

}
