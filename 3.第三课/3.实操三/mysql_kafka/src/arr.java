import java.sql.*;
import java.util.*;

public class arr {
    public List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();

    public List queryAll() {
        Connection conn = null;
        Statement sta = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://bigdata28.depts.bingosoft.net:23307/user25_db?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false", "user25", "pass@bingo25");
            sta = conn.createStatement();
            rs = sta.executeQuery("select * from ngzk");
            ResultSetMetaData md = rs.getMetaData(); //获得结果集结构信息,元数据
            int columnCount = md.getColumnCount();   //获得列数
            while (rs.next()) {
                Map<String,Object> rowData = new HashMap<String,Object>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(rowData);

            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        int size=list.size();
//        String[] array = (String[])list.toArray(new String[size]);
        return list;
    }
}
