# Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.0

## How to package

> mvn clean package -Dmaven.test.skip=true

## How to install in local maven repository

> mvn clean install -Dmaven.test.skip=true

## Using IoTDB JDBC with Maven

```
<dependencies>
    <dependency>
      <groupId>cn.edu.tsinghua</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.3.0</version>
    </dependency>
</dependencies>
```

## Example

```Java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Example {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
        Connection connection = null;
        Statement statement = null;
        try {
            connection =  DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            statement.execute("select s1 from root.laptop.d1");
            ResultSet resultSet = statement.getResultSet();
            while(resultSet.next()){
                System.out.println(String.format("timestamp %s, value %s", resultSet.getString(0), resultSet.getString(1)));
            }
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
}
```
