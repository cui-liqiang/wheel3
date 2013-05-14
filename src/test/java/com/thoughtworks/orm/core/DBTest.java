package com.thoughtworks.orm.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import test.domains.Comment;
import util.DBConnectionTestUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DBTest {
    DBConnectionTestUtil util = new DBConnectionTestUtil();
    DB product;

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        product = DB.connect("product");
    }

    @After
    public void tearDown() throws ClassNotFoundException, SQLException {
        util.executeUpdate("delete from comments;");
        util.close();
    }

    @Test
    public void should_save_object_with_all_fields_set() throws Exception {
        Comment comment = defaultComment();

        product.save(comment);
        verifyObject(comment);
    }

    @Test
    public void should_fill_id_after_saved() throws Exception {
        Comment comment = defaultComment();

        product.save(comment);
        assertTrue(comment.getId() != 0);
    }

    @Test
    public void should_save_object_with_not_all_fields_set() throws Exception {
        Comment comment = defaultComment();
        comment.setEmail(null);

        product.save(comment);
        verifyObject(comment);
    }

    @Test
    public void should_be_able_to_get_object_from_db_by_id() throws Exception {
        Comment comment = defaultComment();
        comment.setEmail(null);
        product.save(comment);

        Comment commentInDb = product.find(Comment.class, comment.getId());
        assertObjsEquality(comment, commentInDb);
    }

    @Test
    public void should_be_able_to_get_count() throws Exception {
        product.save(defaultComment());
        product.save(defaultComment());
        product.save(defaultComment());

        assertEquals(3, product.count(Comment.class));
    }

    @Test
    public void should_be_able_to_delete_obj() throws Exception {
        Comment comment = defaultComment();
        product.save(comment);
        product.delete(comment);
        assertEquals(0, product.count(Comment.class));
    }

    @Test
    public void should_be_able_to_select_by_criteria() throws Exception {
        Comment comment = defaultComment();
        product.save(comment);

        List<Comment> comments = product.findAll(Comment.class, "myuser='Liqiang' and summery='good'");
        assertEquals(1, comments.size());
        assertObjsEquality(comments.get(0), comment);

        assertEquals(0, product.findAll(Comment.class, "myuser='Liqiangs' and summery='good'").size());
    }

    private Comment defaultComment() {
        Comment comment = new Comment();
        comment.setMyUser("Liqiang");
        comment.setSummery("good");
        comment.setWebPage("home page");
        comment.setEmail("xx@xx.com");
        comment.setComments("comment");
        return comment;
    }

    private void assertObjsEquality(Comment comment, Comment commentInDb) {
        assertEquals(comment.getId(), commentInDb.getId());
        assertEquals(comment.getMyUser(), commentInDb.getMyUser());
        assertEquals(comment.getEmail(), commentInDb.getEmail());
        assertEquals(comment.getWebPage(), commentInDb.getWebPage());
        assertEquals(comment.getSummery(), commentInDb.getSummery());
        assertEquals(comment.getComments(), commentInDb.getComments());
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
