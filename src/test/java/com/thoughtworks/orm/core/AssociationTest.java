package com.thoughtworks.orm.core;

import org.junit.Test;
import test.domains.Blog;
import test.domains.Comment;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class AssociationTest extends DBTest{
    Blog blog;

    @Test
    public void should_save_has_many_associations_when_save() throws Exception {
        saveOneBlogWithThreeComments();

        assertEquals(3, product.count(Comment.class));
    }

    @Test
    public void should_save_and_retrieve_belongs_to_association() throws Exception {
        saveOneBlogWithThreeComments();

        for (Comment comment : product.findAll(Comment.class, "")) {
            assertEquals(blog.getId(), comment.getBlog().getId());
        }
    }

    @Test
    public void should_retrieve_has_many_associations_when_find() throws Exception {
        saveOneBlogWithThreeComments();

        Blog blogFromDB = product.find(Blog.class, blog.getId());
        assertEquals(3, blogFromDB.getComments().size());
    }

    private void saveOneBlogWithThreeComments() throws Exception {
        blog = new Blog();

        Comment comment1 = defaultComment();
        Comment comment2 = defaultComment();
        Comment comment3 = defaultComment();
        blog.setComments(newArrayList(comment1, comment2, comment3));

        product.save(blog);
    }
}
