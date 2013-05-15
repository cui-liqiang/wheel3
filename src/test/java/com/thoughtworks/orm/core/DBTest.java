package com.thoughtworks.orm.core;

import org.dom4j.DocumentException;
import org.junit.After;
import org.junit.Before;
import test.domains.Comment;
import util.DBConnectionTestUtil;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DBTest {
    protected DBConnectionTestUtil util = new DBConnectionTestUtil();
    protected DB product;

    @Before
    public void setUp() throws ClassNotFoundException, SQLException, DocumentException {
        DB.init("database.xml");
        product = DB.connect("product");
    }

    @After
    public void tearDown() throws ClassNotFoundException, SQLException {
        util.executeUpdate("delete from comments;");
        util.executeUpdate("delete from blogs;");
        util.close();
    }

    protected Comment defaultComment() {
        Comment comment = new Comment();
        comment.setMyUser("Liqiang");
        comment.setSummery("good");
        comment.setWebPage("home page");
        comment.setEmail("xx@xx.com");
        comment.setComments("comment");
        return comment;
    }

    protected void assertObjsEquality(Comment comment, Comment commentInDb) {
        assertEquals(comment.getId(), commentInDb.getId());
        assertEquals(comment.getMyUser(), commentInDb.getMyUser());
        assertEquals(comment.getEmail(), commentInDb.getEmail());
        assertEquals(comment.getWebPage(), commentInDb.getWebPage());
        assertEquals(comment.getSummery(), commentInDb.getSummery());
        assertEquals(comment.getComments(), commentInDb.getComments());
    }

    protected void verifyObject(Comment comment) throws SQLException {
        ResultSet resultSet = util.executeQuery("select * from comments order by id desc limit 1;");
        assertTrue(resultSet.next());
        assertEquals(comment.getMyUser(), resultSet.getString("myuser"));
        assertEquals(comment.getEmail(), resultSet.getString("email"));
        assertEquals(comment.getWebPage(), resultSet.getString("webpage"));
        assertEquals(comment.getSummery(), resultSet.getString("summery"));
        assertEquals(comment.getComments(), resultSet.getString("comments"));
    }
}
