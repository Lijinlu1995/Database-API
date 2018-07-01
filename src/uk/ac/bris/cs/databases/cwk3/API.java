package uk.ac.bris.cs.databases.cwk3;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.AdvancedForumSummaryView;
import uk.ac.bris.cs.databases.api.AdvancedForumView;
import uk.ac.bris.cs.databases.api.ForumSummaryView;
import uk.ac.bris.cs.databases.api.ForumView;
import uk.ac.bris.cs.databases.api.AdvancedPersonView;
import uk.ac.bris.cs.databases.api.PostView;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.PersonView;
import uk.ac.bris.cs.databases.api.SimpleForumSummaryView;
import uk.ac.bris.cs.databases.api.SimpleTopicView;
import uk.ac.bris.cs.databases.api.TopicView;
import uk.ac.bris.cs.databases.api.*;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

    public API(Connection c) {
        this.c = c;
    }

    /* A.1 */

    @Override
    public Result<Map<String, String>> getUsers() {
        final String sqlstat = "SELECT username, name FROM Person;";
        Map<String, String> map = new HashMap<>();
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            ResultSet resultSet = p.executeQuery();
            while (resultSet.next()) {
                map.put(resultSet.getString("username"), resultSet.getString("name"));
            }
            p.close();
        }
        catch (SQLException e){
            // handle the exception
            return Result.fatal(e.getMessage());
            //throw new UnsupportedOperationException("Not supported yet.");
        }
        return Result.success(map);
    }

    @Override
    public Result<PersonView> getPersonView(String username) {
        if (username == null || username.isEmpty()) {
            return Result.failure("getPersonView: username cannot be empty");
        }
        if (!checkUsername(username)) {
            return Result.failure("getPersonView: username does not exist");
        }
        final String sqlstat = "SELECT * FROM Person WHERE username = ?";
        PersonView pv;
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            String stuId="";
            p.setString(1, username);
            ResultSet resultSet = p.executeQuery();
            resultSet.next();
            if(resultSet.getString(4)!=null){
                stuId=resultSet.getString(4);
            }
            pv = new PersonView(resultSet.getString(2),
                    resultSet.getString(3),stuId);
            p.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(pv);
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        if (name == null || name.isEmpty()) {
            return Result.failure("getPersonView: name cannot be empty");
        }
        if (username == null || username.isEmpty()) {
            return Result.failure("getPersonView: username cannot be empty");
        }
        if (checkUsername(username)) {
            return Result.failure("getPersonView: username already exists");
        }
        if(studentId!=null) {
            if (studentId.isEmpty()) {
                return Result.failure("getPersonView: stuId cannot be empty");
            }
            final String sqlstat1 = "INSERT INTO Person (name, username, stuId) VALUES(?,?,?);";
            try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
                p1.setString(1, name);
                p1.setString(2, username);
                p1.setString(3, studentId);
                p1.execute();
                c.commit();
                p1.close();
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (Exception e1) {
                    return Result.fatal(e1.getMessage());
                }
                return Result.fatal(e.getMessage());
            }
            return Result.success();
        }
        else{
            final String sqlstat2 = "INSERT INTO Person (name, username) VALUES(?, ?);";
            try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                p2.setString(1, name);
                p2.setString(2, username);
                p2.execute();
                c.commit();
                p2.close();
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (Exception e2) {
                    return Result.fatal(e2.getMessage());
                }
                return Result.fatal(e.getMessage());
            }
            return Result.success();
        }
    }

    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
        final String sqlstat = "SELECT id, title FROM SimpleForum ORDER BY title ASC;";
        List<SimpleForumSummaryView> sfsv = new LinkedList<>();
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            ResultSet resultSet = p.executeQuery();
            while (resultSet.next()) {
                sfsv.add(new SimpleForumSummaryView(resultSet.getLong(1),resultSet.getString(2)));
            }
            p.close();
        }
        catch (SQLException e){
            // handle the exception
            return Result.fatal(e.getMessage());
            //throw new UnsupportedOperationException("Not supported yet.");
        }
        return Result.success(sfsv);
    }

    @Override
    public Result createForum(String title) {
        if(title == null || title.isEmpty()){
            return Result.failure("createForum: Forum title cannot be empty");
        }
        List<SimpleForumSummaryView> temp = getSimpleForums().getValue();
        for(int i = 0; i<temp.size(); i++){
            if(temp.get(i).getTitle().equals(title))
                return Result.failure("createForum: Forum title already exists");
        }
        final String sqlstat= "INSERT INTO SimpleForum (title) VALUES(?)";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setString(1,title);
            p.execute();
            c.commit();
            p.close();
        }catch (SQLException e) {
            try {
                c.rollback();
            } catch (Exception f) {
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
        return Result.success();
    }

    /* A.3 */

    @Override
    public Result<List<ForumSummaryView>> getForums() {
        final String sqlstat1 = "SELECT id,title FROM SimpleForum ORDER BY title ASC;";
        List<ForumSummaryView> fsv = new LinkedList<>();
        SimpleTopicSummaryView stsv;
        try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
            ResultSet resultSet1 = p1.executeQuery();
            while (resultSet1.next()) {
                String sqlstat2 = "SELECT t.topicId,t.forumId,t.title\n" +
                        "FROM Topic t\n" +
                        "JOIN Post p\n" +
                        "ON t.topicId=p.topicId \n" +
                        "WHERE t.forumId=?\n" +
                        "ORDER BY p.postedAt DESC LIMIT 1;\n";
                try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                    p2.setLong(1, resultSet1.getLong(1));
                    ResultSet resultSet2 = p2.executeQuery();
                    if (resultSet2.next()) {
                        stsv = new SimpleTopicSummaryView(resultSet2.getLong(1), resultSet2.getLong(2),
                                resultSet2.getString(3));
                    } else {
                        stsv = null;
                    }
                    p2.close();
                } catch (SQLException e) {
                    // handle the exception
                    return Result.fatal(e.getMessage());

                }
                fsv.add(new ForumSummaryView(resultSet1.getLong(1), resultSet1.getString(2), stsv));
            }
            p1.close();
        } catch (SQLException e) {
            // handle the exception
            return Result.fatal(e.getMessage());
        }
        return Result.success(fsv);
    }

    @Override
    public Result<ForumView> getForum(long id) {
        if (String.valueOf(id) == null || String.valueOf(id).isEmpty()) {
            return Result.failure("getForum: id cannot be empty");
        }
        if(!checkForumId(id)){
            return Result.failure("getForum: Forum id does not exist");
        }
        final String sqlstat1 = "SELECT topicId, forumId, title FROM Topic WHERE forumId = ?;";
        List<SimpleTopicSummaryView> stsv = new LinkedList<>();
        ForumView fv;
        try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
            p1.setLong(1,id);
            ResultSet resultSet1 = p1.executeQuery();
            while (resultSet1.next()) {
                stsv.add(new SimpleTopicSummaryView(resultSet1.getLong(1), resultSet1.getLong(2),
                        resultSet1.getString(3)));
            }
            String sqlstat2 = "SELECT sf.id, sf.title FROM SimpleForum sf WHERE sf.id = ?;";
            try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                p2.setLong(1,id);
                ResultSet resultSet2 = p2.executeQuery();
                resultSet2.next();
                fv = new ForumView(resultSet2.getLong(1), resultSet2.getString(2), stsv);
                p2.close();
            } catch (SQLException e) {
                // handle the exception
                return Result.fatal(e.getMessage());
                //throw new UnsupportedOperationException("Not supported yet.");
            }
            p1.close();
        } catch (SQLException e) {
            // handle the exception
            return Result.fatal(e.getMessage());
            //throw new UnsupportedOperationException("Not supported yet.");
        }
        return Result.success(fv);
    }

    @Override
    public Result<SimpleTopicView> getSimpleTopic(long topicId) {
        if(String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()){
            return Result.failure("getSimpleTopic: topicId cannot be empty");
        }
        if(!checkTopicId(topicId)){
            return Result.failure("getSimpleTopic：topicId does not exist");
        }
        final String sqlstat1 = "SELECT topicId,title FROM Topic WHERE topicId=?;";
        SimpleTopicView stv;
        List<SimplePostView> spv = new LinkedList<>();
        try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
            p1.setLong(1, topicId);
            ResultSet resultSet1 = p1.executeQuery();
            resultSet1.next();
            final String sqlstat2 = "SELECT postNumber,authorUsername,text,postedAt FROM Post WHERE topicId=?";
            try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                p2.setLong(1, topicId);
                ResultSet resultSet2 = p2.executeQuery();
                while (resultSet2.next()) {
                    spv.add(new SimplePostView(resultSet2.getInt(1), resultSet2.getString(2),
                            resultSet2.getString(3), resultSet2.getString(4)));
                }
                p2.close();
            } catch (SQLException e) {
                return Result.fatal(e.getMessage());
            }
            stv = new SimpleTopicView(resultSet1.getLong(1), resultSet1.getString(2), spv);
            p1.close();
        } catch (SQLException e) {
            // handle the exception
            return Result.fatal(e.getMessage());
        }
        return Result.success(stv);
    }

    private Boolean checkTopicId(long topicId) {
        final String sqlstat = "SELECT * FROM Topic WHERE topicId = ?;";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, topicId);
            ResultSet resultSet = p.executeQuery();
            if(resultSet.next()){
                return true;
            }
            else{
                return false;
            }
        } catch (SQLException e) {
            throw new Error("checkTopicId: SQLError");
        }
    }

    private Boolean checkUsername(String username) {
        final String sqlstat = "SELECT * FROM Person WHERE username = ?;";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setString(1, username);
            ResultSet resultSet = p.executeQuery();
            if(resultSet.next()){
                return true;
            }
            else{
                return false;
            }
        } catch (SQLException e) {
            throw new Error("checkUsername: SQLError");
        }
    }

    private Boolean checkForumId(long forumId) {
        final String sqlstat = "SELECT * FROM SimpleForum WHERE id = ?;";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, forumId);
            ResultSet resultSet = p.executeQuery();
            if(resultSet.next()){
                return true;
            }
            else{
                return false;
            }
        } catch (SQLException e) {
            throw new Error("checkForumId: SQLError");
        }
    }

    @Override
    public Result<PostView> getLatestPost(long topicId) {
        if (!checkTopicId(topicId)) {
            return Result.failure("getLatstPost: topicId does not exist");
        }
        final String sqlstat ="SELECT sf.id, p.postNumber, pe.name, p.authorUsername, p.text ,p.postedAt\n" +
                "FROM Post p\n" +
                "JOIN Topic t ON p.topicId=t.topicId\n" +
                "JOIN SimpleForum sf ON t.forumId=sf.id \n" +
                "JOIN Person pe ON p.authorUsername=pe.username \n" +
                "WHERE p.topicId=?\n" +
                "ORDER BY p.postedAt DESC LIMIT 1;";
        PostView pv;
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, topicId);
            ResultSet resultSet = p.executeQuery();
            if (resultSet.next()) {
                long fI=resultSet.getLong(1);
                int pN=resultSet.getInt(2);
                String aN=resultSet.getString(3);
                String aU=resultSet.getString(4);
                String t=resultSet.getString(5);
                String pA=resultSet.getString(6);
                int like=countPostLikes(topicId,pN);
                pv=new PostView(fI,topicId,pN,aN,aU,t,pA,like);
                p.close();
            } else {
                return Result.failure("getLatstPost:no posts");
            }

        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(pv);
    }

    @Override
    public Result createPost(long topicId, String username, String text) {
        if (String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()) {
            return Result.failure("createPost: topicId cannot be empty");
        }
        if (username == null || username.isEmpty()) {
            return Result.failure("createPost: username cannot be empty");
        }
        if (text == null || text.isEmpty()) {
            return Result.failure("createPost: text cannot be empty");
        }
        if (!checkTopicId(topicId)) {
            return Result.failure("createPost: topicId does not exist");
        }
        if (!checkUsername(username)) {
            return Result.failure("createPost: username does not exist");
        }
        final String sqlstat = "INSERT INTO Post (topicId, postNumber, authorUsername, text, postedAt) VALUES (?,?,?,?,?);";
        Date dt = new Date();
        String currenttime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dt);
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, topicId);
            p.setInt(2,countPostsInTopic(topicId).getValue()+1);
            p.setString(3, username);
            p.setString(4, text);
            p.setString(5, currenttime);
            p.execute();
            c.commit();
            p.close();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (Exception f) {
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
        return Result.success();
    }

    @Override
    public Result createTopic(long forumId, String username, String title, String text) {
        if (String.valueOf(forumId) == null || String.valueOf(forumId).isEmpty()) {
            return Result.failure("createTopic: forumId cannot be empty");
        }
        if (username == null || username.isEmpty()) {
            return Result.failure("createTopic: username cannot be empty");
        }
        if (title == null || title.isEmpty()) {
            return Result.failure("createTopic: title cannot be empty");
        }
        if (text == null || text.isEmpty()) {
            return Result.failure("createTopic: text cannot be empty");
        }
        if (!checkForumId(forumId)) {
            return Result.failure("createPost: topicId does not exist");
        }
        if (!checkUsername(username)) {
            return Result.failure("createPost: username does not exist");
        }
        final String sqlstat1 = "INSERT INTO Topic(forumId,title) VALUES (?,?);";
        final String sqlstat2 = "SELECT topicId FROM Topic WHERE forumId=? ORDER BY topicId DESC LIMIT 1";
        try {
            PreparedStatement p1 = c.prepareStatement(sqlstat1);
            p1.setLong(1, forumId);
            p1.setString(2, title);
            p1.execute();
            c.commit();

            PreparedStatement p2 = c.prepareStatement(sqlstat2);
            long topicId;
            p2.setLong(1, forumId);
            p2.execute();
            c.commit();
            ResultSet resultSet1 = p2.executeQuery();
            c.commit();
            if (resultSet1.next()) {
                topicId = resultSet1.getLong(1);
            }
            else {
                try {
                    c.rollback();
                    return Result.failure("createTopic error--insert topic error");
                } catch (Exception f1) {
                    return Result.fatal(f1.getMessage());
                }
            }

            if (!createPost(topicId, username, text).isSuccess()) {
                try {
                    c.rollback();
                    return Result.failure("createTopic error--insert post error");
                } catch (Exception f2) {
                    return Result.fatal(f2.getMessage());
                }
            }
            p1.close();
            p2.close();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (Exception f3) {
                return Result.fatal(f3.getMessage());
            }
            return Result.fatal(e.getMessage());
        }
        return Result.success();
    }

    @Override
    public Result<Integer> countPostsInTopic(long topicId) {
        if (String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()) {
            return Result.failure("countPostsInTopic: topicId cannot be empty");
        }
        if(!checkTopicId(topicId)){
            return Result.failure("countPostsInTopic: topicId does not exist");
        }
        final String sqlstat = "SELECT COUNT(*) FROM Post " +
                "JOIN Topic ON Post.topicId = Topic.topicId " +
                "WHERE Topic.topicId = ?";
        Integer cnt;
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, topicId);
            ResultSet resultSet = p.executeQuery();
            resultSet.next();
            cnt = resultSet.getInt(1);
            p.close();
        } catch (SQLException e) {
            // handle the exception
            return Result.fatal(e.getMessage());
            //throw new UnsupportedOperationException("Not supported yet.");
        }
        return Result.success(cnt);
    }

    /* B.1 */

    @Override
    public Result likeTopic(String username, long topicId, boolean like) {
        if (username == null || username.isEmpty()) {
            return Result.failure("likeTopic: username cannot be empty");
        }
        if (String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()) {
            return Result.failure("likeTopic: topicId cannot be empty");
        }
        if (String.valueOf(like) == null || String.valueOf(like).isEmpty()) {
            return Result.failure("likeTopic: like cannot be empty");
        }
        if (!checkUsername(username)) {
            return Result.failure("likePost: username does not exist");
        }
        if (!checkTopicId(topicId)) {
            return Result.failure("likeTopic: topicId does not exist");
        }
        if (like) {
            final String sqlstat1 = "INSERT IGNORE INTO LikeTopic(username,topicId) Values(?,?);";
            try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
                p1.setString(1, username);
                p1.setLong(2, topicId);
                p1.execute();
                c.commit();
                p1.close();
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (Exception f1) {
                    return Result.fatal(f1.getMessage());
                }
                return Result.fatal(e.getMessage());
            }
        }
        else {
            final String sqlstat2 = "DELETE FROM LikeTopic WHERE username=? and topicId=?";
            try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                p2.setString(1, username);
                p2.setLong(2, topicId);
                p2.execute();
                c.commit();
                p2.close();
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (Exception f2) {
                    return Result.fatal(f2.getMessage());
                }
                return Result.fatal(e.getMessage());
            }
        }
        return Result.success();
    }

    @Override
    public Result likePost(String username, long topicId, int post, boolean like) {
        if (username == null || username.isEmpty()) {
            return Result.failure("likePost: username cannot be empty");
        }
        if (String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()) {
            return Result.failure("likePost: topicId cannot be empty");
        }
        if (String.valueOf(post) == null || String.valueOf(post).isEmpty()) {
            return Result.failure("likePost: post cannot be empty");
        }
        if (String.valueOf(like) == null || String.valueOf(like).isEmpty()) {
            return Result.failure("likePost: like cannot be empty");
        }
        if (!checkUsername(username)) {
            return Result.failure("likePost: username does not exist");
        }
        if (!checkTopicId(topicId)) {
            return Result.failure("likePost: topicId does not exist");
        }
        if(!checkPost(topicId,post)){
            return  Result.failure("likePost: post does not exist");
        }
        if (like) {
            final String sqlstat1 = "INSERT IGNORE INTO LikePost(username,postNumber,topicId) Values(?, ?, ?)";
            try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
                p1.setString(1, username);
                p1.setInt(2, post);
                p1.setLong(3, topicId);
                p1.execute();
                c.commit();
                p1.close();
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (Exception f1) {
                    return Result.fatal(f1.getMessage());
                }
                return Result.fatal(e.getMessage());
            }
        }
        else {
            final String sqlstat2 = "DELETE FROM LikePost WHERE username=? and topicId=? and postNumber=?";
            try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                p2.setString(1, username);
                p2.setLong(2, topicId);
                p2.setInt(3, post);
                p2.execute();
                c.commit();
                p2.close();
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (Exception f2) {
                    return Result.fatal(f2.getMessage());
                }
                return Result.fatal(e.getMessage());
            }
        }
        return Result.success();
    }

    private Boolean checkPost(long topicId, int post) {
        final String sqlstat = "SELECT postNumber FROM Post WHERE postNumber = ? AND topicId = ?";
        try (PreparedStatement q = c.prepareStatement(sqlstat)) {
            q.setInt(1,post);
            q.setLong(2,topicId);
            ResultSet resultSet = q.executeQuery();
            if(resultSet.next()){
                return true;
            }
            else{
                return false;
            }
        } catch (SQLException e) {
            throw new Error("checkPost: SQLError");
        }
    }

    @Override
    public Result<List<PersonView>> getLikers(long topicId) {
        if (String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()) {
            return Result.failure("getLikers: topicId cannot be empty");
        }
        if (!checkTopicId(topicId)) {
            return Result.failure("getLikers: topicId does not exist");
        }
        final String sqlstat = "SELECT name, username, stuId FROM Person\n"+
                "JOIN LikeTopic ON Person.username = LikeTopic.username\n" +
                "WHERE topicId = ? ORDER BY name ASC" ;
        List<PersonView> pvlist = new LinkedList<>();
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, topicId);
            ResultSet resultSet = p.executeQuery();
            while (resultSet.next()) {
                PersonView pv = new PersonView(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                pvlist.add(pv);
            }
            p.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(pvlist);
    }

    @Override
    public Result<TopicView> getTopic(long topicId) {
        if (String.valueOf(topicId) == null || String.valueOf(topicId).isEmpty()) {
            return Result.failure("getTopic: topicId cannot be empty");
        }
        if (!checkTopicId(topicId)) {
            return Result.failure("getTopic: topicId does not exist");
        }
        //topicview
        final String sqlstat1 = "SELECT t.forumId, sf.title AS ftitle, t.title AS ttitle\n" +
                "FROM Topic t\n" +
                "JOIN SimpleForum sf \n" +
                "ON t.forumId = sf.id \n" +
                "WHERE t.topicId = ?";
        //postView
        final String sqlstat2 = "SELECT po.postNumber, pe.name, po.authorUsername, po.text, po.postedAt \n" +
                "FROM Post po\n" +
                "JOIN Person pe \n" +
                "ON po.authorUsername = pe.username\n" +
                "WHERE topicId = ? \n" +
                "ORDER BY po.postNumber ASC\n" +
                "LIMIT 10;";
        TopicView tv;
        List<PostView> pv = new LinkedList<>();
        PostView subpv;
        try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
            p1.setLong(1, topicId);
            ResultSet r1 = p1.executeQuery();
            if (r1.next()) {
                try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                    p2.setLong(1, topicId);
                    ResultSet r2 = p2.executeQuery();
                    while (r2.next()) {
                        subpv = new PostView(r1.getLong(1), topicId, r2.getInt(1), r2.getString(2),
                                r2.getString(3), r2.getString(4), r2.getString(5), countPostLikes(topicId, r2.getInt(1)));
                        pv.add(subpv);
                    }
                    p2.close();
                } catch (SQLException e) {
                    return Result.fatal(e.getMessage());
                }
            } else {
                Result.failure("getTopic: No Topic with this id");
            }
            tv = new TopicView(r1.getLong(1), topicId, r1.getString(2), r1.getString(3), pv);
            p1.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(tv);
    }

    private int countPostLikes(long topicId,int postNum) {
        final String sqlstat = "SELECT count(*) FROM LikePost WHERE postNumber = ? AND topicId = ?";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setInt(1, postNum);
            p.setLong(2, topicId);
            ResultSet resultSet = p.executeQuery();
            p.close();
            if (resultSet.next()) return resultSet.getInt(1);
            else return -1;
        } catch (SQLException e) {
            throw new Error("countPostLikes: SQLError");
        }
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        if (username == null || username.isEmpty()) {
            return Result.failure("getAdvancedPersonView: username cannot be empty");
        }
        if (!checkUsername(username)) {
            return Result.failure("likePost: username does not exist");
        }
        AdvancedPersonView apv;
        List<TopicSummaryView> tsv = new LinkedList<>();
        final String sqlstat1 = "SELECT name, username, stuId FROM Person WHERE username = ?";
        final String sqlstat2 = "SELECT t.topicId, t.forumId, t.title FROM Topic t\n" +
                "JOIN LikeTopic lt ON lt.topicId = t.topicId\n" +
                "JOIN Person pe ON pe.username = lt.username\n" +
                "WHERE pe.username = ?;";
        int topicLikes = countPersonLikeTopic(username);
        int postLikes = countPersonLikePost(username);
        String advname, advusername, advstuId;
        try (PreparedStatement p1 = c.prepareStatement(sqlstat1)) {
            p1.setString(1, username);
            ResultSet r1 = p1.executeQuery();
            p1.close();
            if (r1.next()) {
                advname = r1.getString(1);
                advusername = r1.getString(2);
                advstuId = r1.getString(3);
                try (PreparedStatement p2 = c.prepareStatement(sqlstat2)) {
                    p2.setString(1, username);
                    ResultSet r2 = p2.executeQuery();
                    p2.close();
                    while (r2.next()) {
                        Long topicId = r2.getLong(1);
                        String created = getTopic(topicId).getValue().getPosts().get(0).getPostedAt();
                        String lastPostTime = getLatestPost(topicId).getValue().getPostedAt();
                        int postCount = countPostsInTopic(topicId).getValue();
                        String lastPostName = getLatestPost(topicId).getValue().getAuthorUserName();
                        String creatorName = getTopic(topicId).getValue().getPosts().get(0).getAuthorName();
                        String creatorUserName = getTopic(topicId).getValue().getPosts().get(0).getAuthorUserName();
                        int likes = countTopicLikes(topicId);
                        tsv.add(new TopicSummaryView(r2.getLong(1),r2.getLong(2), r2.getString(3),
                                postCount,created, lastPostTime, lastPostName, likes, creatorName, creatorUserName));
                    }
                } catch (SQLException e) {
                    return Result.fatal(e.getMessage());
                }
                apv = new AdvancedPersonView(advname, advusername, advstuId, topicLikes, postLikes,tsv);
            }
            else return Result.failure("No user with this username");
        }catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(apv);
    }

    private int countPersonLikeTopic(String username){
        final String sqlstat = "SELECT COUNT(*) \n" +
                "FROM Person pe\n" +
                "JOIN LikeTopic lt \n" +
                "ON pe.username=lt.username\n" +
                "WHERE pe.username= ?;";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setString(1,username);
            ResultSet resultSet = p.executeQuery();
            p.close();
            if (resultSet.next()) return resultSet.getInt(1);
            else return -1;
        } catch (SQLException e) {
            throw new Error("countPersonLikeTopic: SQLError");
        }
    }

    private int countPersonLikePost(String username){
        final String sqlstat = "SELECT COUNT(*) \n" +
                "FROM Person pe\n" +
                "JOIN LikePost lp \n" +
                "ON pe.username=lp.username\n" +
                "WHERE pe.username= ?;";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setString(1,username);
            ResultSet resultSet = p.executeQuery();
            p.close();
            if (resultSet.next()) return resultSet.getInt(1);
            else return -1;
        } catch (SQLException e) {
            throw new Error("countPersonLikePost: SQLError");
        }
    }

    private int countTopicLikes(long topicId) {
        final String sqlstat = "SELECT count(*) FROM LikeTopic WHERE topicId = ?";
        try (PreparedStatement p = c.prepareStatement(sqlstat)) {
            p.setLong(1, topicId);
            ResultSet resultSet = p.executeQuery();
            p.close();
            if (resultSet.next()) return resultSet.getInt(1);
            else return -1;
        } catch (SQLException e) {
            throw new Error("countTopicLikes: SQLError");
        }
    }



    @Override
    public Result<AdvancedForumView> getAdvancedForum(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
