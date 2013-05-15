package com.thoughtworks.orm.core;

import org.junit.Test;
import test.domains.Blog;
import test.domains.Comment;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class AssociationTest extends DBTest{
    @Test
    public void should_save_associations_when_save() throws Exception {
        Blog blog = new Blog();

        Comment comment1 = defaultComment();
        Comment comment2 = defaultComment();
        Comment comment3 = defaultComment();
        blog.setComments(newArrayList(comment1, comment2, comment3));

        product.save(blog);

        assertEquals(3, product.count(Comment.class));
    }
}
