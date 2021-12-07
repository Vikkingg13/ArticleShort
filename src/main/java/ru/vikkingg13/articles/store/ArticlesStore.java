package ru.vikkingg13.articles.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class ArticlesStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArticlesStore.class.getSimpleName());

    private static Properties loadProperties() {
        LOGGER.info("Загрузка настроек приложения");
        var properties = new Properties();
        try (InputStream in = ArticlesStore.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(in);
        } catch (Exception e) {
            LOGGER.error("Не удалось загрузить настройки. { }", e.getCause());
            throw new IllegalStateException();
        }
        return properties;
    }

    public void run() throws SQLException {
        Properties properties = loadProperties();

        LOGGER.info("Создание подключения к БД статей");
        Connection connection = DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("username"),
                properties.getProperty("password")
        );
        var statement = connection.createStatement();
        LOGGER.info("Инициализация таблицы статей");
        var sql = "create table if not exists articles ("
                 + "id serial primary key,"
                 + "text text);";
        statement.execute(sql);
        statement.close();

        int count = 1000;
        int batchSize = 10;
        List<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            LOGGER.info("Генерация строк");
            String string = new Random().ints()
                        .limit(1_000_000)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining());
            list.add(string);
            if ((i % batchSize) == 0) {
                sql = "insert into articles(text) values(?)";
                var prepareStatement = connection.prepareStatement(sql);
                connection.setAutoCommit(false);
                for (String str : list) {
                    LOGGER.info("Сохранение строк");
                    prepareStatement.setString(1, str);
                    prepareStatement.executeUpdate();
                }
                connection.commit();
                list.clear();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        new ArticlesStore().run();
    }
}
