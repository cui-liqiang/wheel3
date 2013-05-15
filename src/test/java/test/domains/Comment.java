package test.domains;

import com.thoughtworks.orm.annotation.BelongsTo;
import com.thoughtworks.orm.annotation.PrimaryKey;

public class Comment {
    private int id;
    private String myUser;
    private String email;
    private String webPage;
    private String summery;
    private String comments;
    private Blog blog;

    @BelongsTo
    public Blog getBlog() {
        return blog;
    }

    public void setBlog(Blog blog) {
        this.blog = blog;
    }

    @PrimaryKey
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMyUser() {
        return myUser;
    }

    public void setMyUser(String myUser) {
        this.myUser = myUser;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getWebPage() {
        return webPage;
    }

    public void setWebPage(String webPage) {
        this.webPage = webPage;
    }

    public String getSummery() {
        return summery;
    }

    public void setSummery(String summery) {
        this.summery = summery;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }
}
