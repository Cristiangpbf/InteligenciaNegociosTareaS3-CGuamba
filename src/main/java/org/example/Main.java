package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * CGuamba -> Creación de un almacen histórico medainte un proceso ETL.
 */
public class Main {

    public static void main(String[] args) {

        String localDbUrl = "jdbc:postgresql://localhost:5432/postgres";
        String localDbUser = "postgres";
        String localDbPassword = "ADMIN";

        String cloudDbUrl = "jdbc:postgresql://prueba-nteligencia-negocios.postgres.database.azure.com:5432/postgres";
        String cloudDbUser = "cristian";
        String cloudDbPassword = "Ripazha@24067";

        List<Sale> sales = extractData(localDbUrl, localDbUser, localDbPassword);
        List<HistoricalSale> historicalSales = transformData(sales);
        loadData(cloudDbUrl, cloudDbUser, cloudDbPassword, historicalSales);

    }

    private static List<Sale> extractData(String url, String user, String password) {
        List<Sale> sales = new ArrayList<>();
        String query = "SELECT * FROM sales";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                Sale sale = new Sale();
                sale.setId(rs.getInt("id"));
                sale.setProductId(rs.getInt("product_id"));
                sale.setSaleDate(rs.getDate("sale_date"));
                sale.setQuantity(rs.getInt("quantity"));
                sale.setPrice(rs.getBigDecimal("price"));
                sales.add(sale);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return sales;
    }

    private static List<HistoricalSale> transformData(List<Sale> sales) {
        List<HistoricalSale> historicalSales = new ArrayList<>();

        for (Sale sale : sales) {
            HistoricalSale historicalSale = new HistoricalSale();
            historicalSale.setId(sale.getId());
            historicalSale.setProductId(sale.getProductId());
            historicalSale.setSaleDate(sale.getSaleDate());
            historicalSale.setQuantity(sale.getQuantity());
            historicalSale.setPrice(sale.getPrice());
            historicalSale.setInsertedAt(new Timestamp(System.currentTimeMillis()));
            historicalSales.add(historicalSale);
        }

        return historicalSales;
    }

    private static void loadData(String url, String user, String password, List<HistoricalSale> historicalSales) {
        String insertQuery = "INSERT INTO historical_sales (id, product_id, sale_date, quantity, price, inserted_at) \n" +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {

            for (HistoricalSale historicalSale : historicalSales) {
                pstmt.setInt(1, historicalSale.getId());
                pstmt.setInt(2, historicalSale.getProductId());
                pstmt.setDate(3, historicalSale.getSaleDate());
                pstmt.setInt(4, historicalSale.getQuantity());
                pstmt.setBigDecimal(5, historicalSale.getPrice());
                pstmt.setTimestamp(6, historicalSale.getInsertedAt());
                pstmt.addBatch();
            }

            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}