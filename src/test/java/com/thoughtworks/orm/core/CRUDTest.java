package com.thoughtworks.orm.core;

import org.junit.Test;
import test.domains.Comment;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CRUDTest extends DBTest {
    @Test
    public void should_save_object_with_all_fields_set() throws Exception {
        Comment comment = defaultComment();

        product.save(comment);
        verifyObject(comment);
    }

    @Test
    public void should_update_object() throws Exception {
        Comment comment = defaultComment();

        product.save(comment);
        Comment comment1 = product.find(Comment.class, comment.getId());
        comment1.setComments("changed");
        product.save(comment1);
        Comment comment2 = product.find(Comment.class, comment.getId());
        assertObjsEquality(comment1, comment2);
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
}
