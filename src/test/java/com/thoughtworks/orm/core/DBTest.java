package com.thoughtworks.orm.core;

import org.junit.After;
import org.junit.Test;
import test.domains.Comment;
import util.DBConnectionTestUtil;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DBTest {
    DBConnectionTestUtil util = new DBConnectionTestUtil();

    @After
    public void tearDown() {
        util.executeUpdate("delete from comments;");
        util.close();
    }

    @Test
    public void should_save_object_with_all_fields_set() throws Exception {
        DB product = DB.connect("product");
        Comment comment = new Comment();
        comment.setMyUser("Liqiang");
        comment.setSummery("good");
        comment.setWebPage("home page");
        comment.setEmail("xx@xx.com");
        comment.setComments("comment");

        product.save(comment);
        verifyObject(comment);
    }

    @Test
    public void should_save_object_with_not_all_fields_set() throws Exception {
        DB product = DB.connect("product");
        Comment comment = new Comment();
        comment.setMyUser("Liqiang");
        comment.setSummery("good");
        comment.setWebPage("home page");
        comment.setComments("comment");

        product.save(comment);
        verifyObject(comment);
    }

    private void verifyObject(Comment comment) throws SQLException {
        ResultSet resultSet = util.executeQuery("select * from comments order by id desc limit 1;");
        assertTrue(resultSet.next());
        assertEquals(comment.getMyUser(), resultSet.getString("myuser"));
        assertEquals(comment.getEmail(), resultSet.getString("email"));
        assertEquals(comment.getWebPage(), resultSet.getString("webpage"));
        assertEquals(comment.getSummery(), resultSet.getString("summery"));
        assertEquals(comment.getComments(), resultSet.getString("comments"));
    }
}
