package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@SpringBootApplication
public class SimpleBatch implements CommandLineRunner {

	@Autowired
	PlatformTransactionManager transactionManager;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Value("${delete.commit.interval}")
	int commitInterval;

	private final static Logger log = LoggerFactory
			.getLogger(SimpleBatch.class);

	public static void main(String[] args) {
		try {
			System.exit(SpringApplication.exit(
					SpringApplication.run(SimpleBatch.class, args)));
		} catch (Exception e) {
			log.error(e.getMessage());
			System.exit(1);
		}
	}

	@Override
	public void run(String... args) throws Exception {
		int count;
		while ((count = delete()) != 0) {

			log.info("deleted " + count + " records.");
			log.info(jdbcTemplate.queryForObject("SELECT count(*) FROM people",
					Integer.class) + " records remain");
		}
	}

	private int delete() {
		TransactionStatus transactionStatus = transactionManager
				.getTransaction(new DefaultTransactionDefinition());

		try {
			int count;
			count = jdbcTemplate.update(
					"DELETE FROM people where person_id in (select person_id from people limit ?)",
					commitInterval);
			transactionManager.commit(transactionStatus);
			return count;
		} catch (Exception e) {
			transactionManager.rollback(transactionStatus);
			throw e;
		}
	}
}
