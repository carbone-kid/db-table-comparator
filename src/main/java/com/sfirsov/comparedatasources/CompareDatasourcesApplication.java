package com.sfirsov.comparedatasources;

import com.opencsv.CSVReader;
import com.sfirsov.comparedatasources.model.DSProperties;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
public class CompareDatasourcesApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(CompareDatasourcesApplication.class, args);
	}

	public List<DSProperties> readCSV(String filename) {
		System.out.println("Reading datasources:");
		List<DSProperties> properties = new ArrayList<>();
		try(CSVReader reader = new CSVReader(new FileReader(filename))) {
			String[] line;
			while ((line = reader.readNext()) != null) {
				properties.add(new DSProperties(line[0], line[1], line[2]));
				System.out.println(line[0]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return properties;
	}

	List<Map<String, Object>> readDataFromDatabase(DSProperties properties) {
		DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
		dataSourceBuilder.url(properties.getUrl().trim());
		dataSourceBuilder.username(properties.getUsername().trim());
		dataSourceBuilder.password(properties.getPassword().trim());
		dataSourceBuilder.driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		DataSource dataSource = dataSourceBuilder.build();

		JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		String query = "select * from test_table";
		try {
			list = jdbcTemplate.queryForList(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("PostgreSQL Data: " + list.toString());

		return list;
	}

	List<Map<String, Object>> getIntersectionList(List<Map<String, Object>> first, List<Map<String, Object>> second) {
		List<Map<String, Object>> intersectList = new ArrayList<Map<String, Object>>();
		for(Map<String, Object> f  : first) {
			for(Map<String, Object> s  : second) {
				if(f.get("name").equals(s.get("name"))) {
					intersectList.add(f);
					break;
				}
			}
		}
		return intersectList;
	}

	List<Map<String, Object>> getDifferenceListOneWay(List<Map<String, Object>> first, List<Map<String, Object>> second) {
		List<Map<String, Object>> diffList = new ArrayList<Map<String, Object>>();
		for(Map<String, Object> f  : first) {
			boolean exist = false;
			for(Map<String, Object> s  : second) {
				if(f.get("name").equals(s.get("name"))) {
					exist = true;
					break;
				}
			}
			if(!exist) {
				diffList.add(f);
			}
		}
		return diffList;
	}

	List<Map<String, Object>> getDifferenceList(List<Map<String, Object>> first, List<Map<String, Object>> second) {
		List<Map<String, Object>> r1 = getDifferenceListOneWay(first, second);
		r1.addAll(getDifferenceListOneWay(second, first));
		return r1;
	}

	@Override
	public void run(String... args) throws Exception {
		if(args.length < 1) {
			System.out.println("please specify csv file with databases connection parameters like this:");
			System.out.println("jdbc:sqlserver://localhost:1433, username, password");
			System.out.println("jdbc:sqlserver://localhost:1434, username, password");
			return;
		}

		List<DSProperties> propertiesList = readCSV(args[0]);

		if(propertiesList.isEmpty()) {
			System.out.println("No datasourse properties found in the file.");
			return;
		}

		List<Map<String, Object>> firstList = readDataFromDatabase(propertiesList.get(0));
		List<Map<String, Object>> secondList = readDataFromDatabase(propertiesList.get(1));

		List<Map<String, Object>> diff = getDifferenceList(firstList, secondList);
		List<Map<String, Object>> intersect = getIntersectionList(firstList, secondList);
	}
}
