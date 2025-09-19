package com.oracle;


import java.sql.*;
import java.util.*;
import java.io.*;

public class Main {

    private static final String SCHEMA = "INSIS_GEN_CFG_V10";
    private static final String OUTPUT_DOT =  File.separator + "schema.dot";
    public static void main(String[] args) {
        chargeSchema();
    }

    public static void chargeSchema() {

        Connection conn = Connect.getConnection();

        try {
   
            System.out.println("Carpeta de salida: " + LoadConfig.outputDir);

            DatabaseMetaData meta = conn.getMetaData();

            // 1) Leer tablas del esquema
            List<String> tables = loadTables(meta, SCHEMA);
            System.out.println("Tablas encontradas: " + tables.size());

            // 2) Obtener FKs y validar relaciones
            Map<String, TableNode> nodes = new HashMap<>();
            for (String t : tables) {
                nodes.put(t, new TableNode(t));
            }

            // edgeKey: constraintName@childTable -> Relationship object
            Map<String, Relationship> relationships = new LinkedHashMap<>();

            for (String table : tables) {
                ResultSet fkRs = meta.getImportedKeys(null, SCHEMA, table);
                while (fkRs.next()) {
                    String pkTable = fkRs.getString("PKTABLE_NAME");
                    String pkColumn = fkRs.getString("PKCOLUMN_NAME");
                    String fkTable = fkRs.getString("FKTABLE_NAME");
                    String fkColumn = fkRs.getString("FKCOLUMN_NAME");
                    String fkName = fkRs.getString("FK_NAME"); // nombre de constraint
                    short keySeq = fkRs.getShort("KEY_SEQ");
                    // Formar una key por constraint (algunas FKs son compuestas)
                    String relKey = (fkName != null ? fkName : ("FK_"+pkTable+"_"+fkTable)) + "@" + fkTable;

                    Relationship rel = relationships.get(relKey);
                    if (rel == null) {
                        rel = new Relationship(pkTable, fkTable, fkName);
                        relationships.put(relKey, rel);
                    }
                    rel.addColumnMapping(keySeq, pkColumn, fkColumn);
                }
                fkRs.close();
            }

            // Validar cada relación (conteo columnas, existencia de tablas y columnas basico)
            validateRelationships(conn, SCHEMA, relationships, nodes);

            // Generar DOT
            writeDotFile(LoadConfig.outputDir + OUTPUT_DOT, nodes, relationships);
            System.out.println("Archivo DOT generado: " + OUTPUT_DOT);
            System.out.println("Para generar imagen (si tienes Graphviz instalado):");
            System.out.println("  dot -Tpng " + OUTPUT_DOT + " -o schema.png");
        } catch (SQLException e) {
            System.err.println("Error SQL: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Error I/O: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (conn != null) try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    private static List<String> loadTables(DatabaseMetaData meta, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        // Obtener solo tablas (no views)
        ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"});
        while (rs.next()) {
            String tableName = rs.getString("TABLE_NAME");
            tables.add(tableName);
        }
        rs.close();
        Collections.sort(tables);
        return tables;
    }

    private static void validateRelationships(Connection conn, String schema,
                                              Map<String, Relationship> relationships,
                                              Map<String, TableNode> nodes) {
        System.out.println("\nValidando relaciones encontradas...");
        for (Map.Entry<String, Relationship> e : relationships.entrySet()) {
            Relationship rel = e.getValue();
            System.out.println("\nConstraint: " + (rel.fkName != null ? rel.fkName : "(sin nombre)"));
            System.out.println("  Padre: " + rel.parentTable + "   Hija: " + rel.childTable);

            // validar que ambas tablas existan en nodes
            if (!nodes.containsKey(rel.parentTable)) {
                System.out.println("  [ERROR] Tabla padre no encontrada en listado: " + rel.parentTable);
            }
            if (!nodes.containsKey(rel.childTable)) {
                System.out.println("  [ERROR] Tabla hija no encontrada en listado: " + rel.childTable);
            }

            // validar columnas y conteo
            List<ColumnPair> pairs = rel.getOrderedMappings();
            System.out.println("  Columnas mapeadas (" + pairs.size() + "):");
            for (ColumnPair p : pairs) {
                System.out.println("    " + p.parentCol + "  <-  " + p.childCol);
            }

            // (Opcional) Comprobar que columnas existen y tipos compatibles (básico)
            try {
                Map<String, String> parentCols = getColumnTypes(conn, schema, rel.parentTable);
                Map<String, String> childCols  = getColumnTypes(conn, schema, rel.childTable);
                boolean mismatch = false;
                for (ColumnPair p : pairs) {
                    if (!parentCols.containsKey(p.parentCol)) {
                        System.out.println("    [ERROR] Columna padre no encontrada: " + p.parentCol);
                        mismatch = true;
                    }
                    if (!childCols.containsKey(p.childCol)) {
                        System.out.println("    [ERROR] Columna hija no encontrada: " + p.childCol);
                        mismatch = true;
                    }
                    if (!mismatch) {
                        String pt = parentCols.get(p.parentCol);
                        String ct = childCols.get(p.childCol);
                        if (pt != null && ct != null && !areTypesCompatible(pt, ct)) {
                            System.out.println("    [WARN] Posible incompatibilidad de tipos: padre(" + p.parentCol + ":" + pt + ") vs hija(" + p.childCol + ":" + ct + ")");
                        }
                    }
                }
                if (!mismatch) {
                    System.out.println("  Validación básica de columnas: OK");
                }
            } catch (SQLException ex) {
                System.out.println("  [WARN] No se pudo validar tipos de columnas: " + ex.getMessage());
            }
        }
    }

    private static Map<String, String> getColumnTypes(Connection conn, String schema, String table) throws SQLException {
        Map<String, String> map = new HashMap<>();
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getColumns(null, schema, table, "%");
        while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            int size = rs.getInt("COLUMN_SIZE");
            map.put(colName, typeName + (size > 0 ? "(" + size + ")" : ""));
        }
        rs.close();
        return map;
    }

    private static boolean areTypesCompatible(String t1, String t2) {
        String a = t1.toUpperCase();
        String b = t2.toUpperCase();
        if (a.startsWith("VARCHAR") || a.startsWith("CHAR") || a.startsWith("NVARCHAR")) {
            return b.startsWith("VARCHAR") || b.startsWith("CHAR") || b.startsWith("NVARCHAR");
        }
        if (a.startsWith("NUMBER") || a.startsWith("DECIMAL") || a.startsWith("INT")) {
            return b.startsWith("NUMBER") || b.startsWith("DECIMAL") || b.startsWith("INT");
        }
        if (a.startsWith("DATE") || a.startsWith("TIMESTAMP")) {
            return b.startsWith("DATE") || b.startsWith("TIMESTAMP");
        }
        String pa = a.split("[\\(]")[0];
        String pb = b.split("[\\(]")[0];
        return pa.equals(pb);
    }

    private static void writeDotFile(String filename, Map<String, TableNode> nodes, Map<String, Relationship> relationships) throws IOException {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(filename))) {
            w.write("digraph schema {\n");
            w.write("  graph [rankdir=LR, splines=true];\n");
            w.write("  node [shape=box, style=filled, fillcolor=\"white\"];\n\n");

            for (String table : nodes.keySet()) {
                w.write("  \"" + table + "\";\n");
            }
            w.write("\n");
            for (Relationship rel : relationships.values()) {
                String parent = rel.parentTable;
                String child = rel.childTable;
                List<ColumnPair> pairs = rel.getOrderedMappings();
                String label = buildLabelFromPairs(pairs);
                w.write("  \"" + parent + "\" -> \"" + child + "\" [ label=\"" + escapeDot(label) + "\" ];\n");
            }

            w.write("}\n");
        }
    }

    private static String buildLabelFromPairs(List<ColumnPair> pairs) {
        if (pairs.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (ColumnPair p : pairs) {
            if (!first) sb.append(", ");
            sb.append(p.parentCol).append(" <= ").append(p.childCol);
            first = false;
        }
        return sb.toString();
    }

    private static String escapeDot(String s) {
        return s.replace("\"", "\\\"");
    }

    private static class TableNode {
        String name;
        TableNode(String name) { this.name = name; }
    }

    private static class Relationship {
        final String parentTable;
        final String childTable;
        final String fkName;
        final Map<Integer, ColumnPair> mappings = new TreeMap<>();

        Relationship(String parent, String child, String fkName) {
            this.parentTable = parent;
            this.childTable = child;
            this.fkName = fkName;
        }

        void addColumnMapping(int seq, String parentCol, String childCol) {
            mappings.put(seq, new ColumnPair(parentCol, childCol));
        }

        List<ColumnPair> getOrderedMappings() {
            return new ArrayList<>(mappings.values());
        }
    }

    private static class ColumnPair {
        final String parentCol;
        final String childCol;
        ColumnPair(String parentCol, String childCol) {
            this.parentCol = parentCol;
            this.childCol = childCol;
        }
    }
    
}

