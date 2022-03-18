package hive;

import java.sql.*;

/**
 * 需要依赖hadoop和hive的jar包
 * 服务器需要启动  hive --service hiveserver2
 */
public class HiveJdbcClient {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection conn = DriverManager.getConnection("jdbc:hive2://node01:10000/default", "root", "");
		Statement stmt = conn.createStatement();
		String sql = "select * from psn6 limit 5";
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "-" + res.getString("name"));
		}
	}
}
