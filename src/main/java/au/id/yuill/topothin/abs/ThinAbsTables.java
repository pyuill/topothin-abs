/*
 * Copyright (c) 2017 Peter Yuill
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 */
package au.id.yuill.topothin.abs;

import au.id.yuill.topothin.DefaultSimplifier;
import au.id.yuill.topothin.Row;
import au.id.yuill.topothin.Table;
import au.id.yuill.topothin.TopoCoordData;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryCombiner;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Load ABS data for postcode (poa), local government area (lga), state electoral division (sed)
 * and commonwealth electoral division (ced).
 * <br>Find nodes in all LineStrings, find edges, simplify edges, create simplified polygons
 * and save thinned geometry back to the display tables. Also create GeoJSON versions of the
 * features. Lastly create polygons for states and save them.
 * <br><br>This program depends on BuildTables to create the display versions of admin tables.
 *
 * @version 1.0
 * @author Peter Yuill
 */
public class ThinAbsTables {

    public static String poaReleaseYear;
    public static String lgaReleaseYear;
    public static String sedReleaseYear;
    public static String cedReleaseYear;
    public static String dbUrl;
    public static String dbUser;
    public static String dbPass;
    public static Connection conn;
    public static Statement stmt;

    public static void main(String[] args) throws Exception {
        if (args.length == 7) {
            poaReleaseYear = args[0];
            lgaReleaseYear = args[1];
            sedReleaseYear = args[2];
            cedReleaseYear = args[3];
            dbUrl = args[4];
            dbUser = args[5];
            dbPass = args[6];
        } else {
            System.out.println("usage: ThinAbsTables poaReleaseYear lgaReleaseYear sedReleaseYear cedReleaseYear dbUrl dbUser dbPassword");
            System.exit(0);
        }
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);

        WKBReader reader = new WKBReader();
        WKBWriter writer = new WKBWriter(2, true);
        TopoCoordData tcd = new TopoCoordData(new DefaultSimplifier(), 4283);

        Table poaTable = new AbsTable(poaReleaseYear, "poa", null);
        Table lgaTable = new LgaTable(lgaReleaseYear, null);
        Table sedTable = new AbsTable(sedReleaseYear, "sed", null);
        Table cedTable = new AbsTable(cedReleaseYear, "ced", null);

        poaTable.populateTopoCoordData(conn, reader, tcd);
        lgaTable.populateTopoCoordData(conn, reader, tcd);
        sedTable.populateTopoCoordData(conn, reader, tcd);
        cedTable.populateTopoCoordData(conn, reader, tcd);

        System.out.println("Find nodes");
        tcd.findNodes();

        System.out.println("Create Edges");
        tcd.createEdges();

        System.out.println("Simplify Edges");
        tcd.simplifyEdges();

        System.out.println("Reassemble Polygons");
        tcd.createThinnedPolygons();

        poaTable.saveThinnedGeometry(conn, writer, tcd);
        lgaTable.saveThinnedGeometry(conn, writer, tcd);
        sedTable.saveThinnedGeometry(conn, writer, tcd);
        cedTable.saveThinnedGeometry(conn, writer, tcd);

        System.out.println("Create States");
        PreparedStatement ps = conn.prepareStatement("update ste_disp set geom = ? where ste_code = ?");

        List<Row> lgaList = tcd.tableMap.get(lgaTable);
        Map<String, List<LgaRow>> stateMap = new HashMap();
        for (Row row: lgaList) {
            LgaRow lga = (LgaRow)row;
            List<LgaRow> stateList = stateMap.get(lga.stateCode);
            if (stateList == null) {
                stateList = new ArrayList();
                stateMap.put(lga.stateCode, stateList);
            }
            stateList.add(lga);
        }

        for (String steCode: stateMap.keySet()) {
            List<MultiPolygon> mpList = new ArrayList();
            for (LgaRow lgaRow: stateMap.get(steCode)) {
                mpList.add(lgaRow.mp);
            }
            Geometry comb = GeometryCombiner.combine(mpList);
            System.out.println(steCode + " Combined " + comb.getClass().getName() + " " + comb.getSRID() + " " + comb.getNumGeometries());
            Geometry state = comb.buffer(0.0);
            System.out.println(steCode + " Buffer " + state.getClass().getName() + " " + state.getSRID() + " " + state.getNumGeometries());
            if (state instanceof Polygon) {
                state = new MultiPolygon(new Polygon[] {(Polygon)state}, tcd.factory);
            }
            ps.setBytes(1, writer.write(state));
            ps.setString(2, steCode);
            ps.execute();
        }
        Statement stmt = conn.createStatement();
        stmt.execute("update ste_disp set geojson = ST_AsGeoJSON(geom,6,0) where geom is not null");
        stmt.close();
        conn.close();
    }
}
