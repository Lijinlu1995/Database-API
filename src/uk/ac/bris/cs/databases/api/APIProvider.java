package uk.ac.bris.cs.databases.api;

import java.util.List;
import java.util.Map;

/**
 * This is the main interface that you have to implement.
 * 
 * The methods are divided in to two groups "A" and "B". You should implement
 * all methods of group "A" and only attempt the "B" methods if you wish to aim
 * for higher marks AND you are confident about your group "A" solutions.
 * 
 * Implementing all group "A" methods well will get you more marks than doing
 * the "B" ones as well but with mistakes in both groups.
 * 
 * In addition, methods have a difficulty from one to three stars that indicates
 * very roughly how challenging they are to implement.
 * 
 * @author csxdb
 */
public interface APIProvider {
    
    /*
     * Group A - every group is expected to implement all of these methods.
     */

    /* 
     * A.1 "Person" methods
     * It is recommended to implement these three methods first.
     */
    
    /**
     * Get a list of all users in the system as a map username -> name.
     * @return A map with one entry per user of the form username -> name
     * (note that usernames are unique).
     * 
     * Difficulty: *
     * Used by: /people (PeopleHandler)
     */
    public Result<Map<String, String>> getUsers();
    
    /**
     * Get a PersonView for the person with the given username.
     * @param username - the username to search for, cannot be empty.
     * @return If a person with the given username exists, a fully populated
     * PersonView. Otherwise, failure (or fatal on a database error).
     * 
     * Difficulty: *
     * Used by: /person/:id (PersonHandler)
     */
    public Result<PersonView> getPersonView(String username);
       
    /**
     * Create a new person.
     * @param name - the person's name, cannot be empty.
     * @param username - the person's username, cannot be empty.
     * @param studentId - the person's student id. May be either NULL if the
     * person is not a student or a non-empty string if they are; can not be
     * an empty string.
     * @return Success if no person with this username existed yet and a new
     * one was created, failure if a person with this username already exists,
     * fatal if something else went wrong.
     * 
     * Difficulty: **
     * Used by: /newperson => /createperson (CreatePersonHandler)
     */
    public Result addNewPerson(String name, String username, String studentId);
    
    /*
     * A.2 Forums only (no topics needed yet).
     * Create some sample data in your database manually to test getSimpleForums.
     * Then you can implement createForum and check that the two work together.
     */
    
    /**
     * Get the "main page" containing a list of forums ordered alphabetically
     * by title. Simple version that does not return any topic information.
     * @return the list of all forums; an empty list if there are no forums.
     * 
     * Difficulty: *
     * Used by: /forums0 (SimpleForumsHandler)
     */
    public Result<List<SimpleForumSummaryView>> getSimpleForums();

    /**
     * Create a new forum.
     * @param title - the title of the forum. Must not be null or empty and
     * no forum with this name must exist yet.
     * @return success if the forum was created, failure if the title was
     * null, empty or such a forum already existed; fatal on other errors.
     * 
     * Difficulty: **
     * Used by: /newforum => /createforum (CreateForumHandler)
     */
    public Result createForum(String title);
    
    /*
     * A.3 Forums, topics, posts.
     */
    
    /**
     * Get the "main page" containing a list of forums ordered alphabetically
     * by title.
     * @return the list of all forums, empty list if there are none.
     * 
     * Difficulty: **
     * Used by: /forums (ForumsHandler)
     */
    public Result<List<ForumSummaryView>> getForums();
    
    /**
     * Get the detailed view of a single forum.
     * @param id - the id of the forum to get.
     * @return A view of this forum if it exists, otherwise failure.
     * 
     * Difficulty: **
     * Used by: /forum/:id (ForumHandler)
     */
    public Result<ForumView> getForum(long id);
    
    /**
     * Get a simplified view of a topic.
     * @param topicId - the topic to get.
     * @return The topic view if one exists with the given id,
     * otherwise failure or fatal on database errors. 
     * 
     * Difficulty: **
     * Used by: /topic0/:id (SimpleTopicHandler)
     */
    public Result<SimpleTopicView> getSimpleTopic(long topicId);

    /**
     * Get the latest post in a topic.
     * @param topicId The topic. Must exist.
     * @return Success and a view of the latest post if one exists,
     * failure if the topic does not exist, fatal on database errors.
     * 
     * Difficulty: **
     * Not used in web interface.
     */
    public Result<PostView> getLatestPost(long topicId);
    
    /**
     * Create a post in an existing topic.
     * @param topicId - the id of the topic to post in. Must refer to
     * an existing topic.
     * @param username - the name under which to post; user must exist.
     * @param text - the content of the post, cannot be empty.
     * @return success if the post was made, failure if any of the preconditions
     * were not met and fatal if something else went wrong.
     * 
     * Difficulty: ** to *** depending on schema
     * Used by: /newpost/:id => /createpost (CreatePostHandler)
     */
    public Result createPost(long topicId, String username, String text);
    
    /**
     * Create a new topic in a forum.
     * @param forumId - the id of the forum in which to create the topic. This
     * forum must exist.
     * @param username - the username under which to make this post. Must refer
     * to an existing username.
     * @param title - the title of this topic. Cannot be empty.
     * @param text - the text of the initial post. Cannot be empty.
     * @return failure if any of the preconditions are not met (forum does not
     * exist, user does not exist, title or text empty);
     * success if the post was created and fatal if something else went wrong.
     * 
     * Difficulty: ***
     * Used by: /newtopic/:id => /createtopic (CreateTopicHandler)
     */
    public Result createTopic(long forumId, String username, String title,
            String text);
     
    /**
     * Count the number of posts in a topic (without fetching them all).
     * @param topicId - the topic to look at.
     * @return The number of posts in this topic if it exists, otherwise a
     * failure.
     * 
     * Difficulty: *
     * Not used in web interface.
     */
    public Result<Integer> countPostsInTopic(long topicId);
    
    /*
     * Group "B" - optional methods.
     * ONLY implement these for extra marks if you are confident about your
     * group "A" methods. Implementing all of group "A" well gets you higher
     * marks than implementing both groups with significant mistakes.
     */

    /*
     * B.1 "likes"
     */
    
    /**
     * Like or unlike a topic. A topic is either liked or not, when calling this
     * twice in a row with the same parameters, the second call is a no-op (this
     * function is idempotent).
     * @param username - the person liking the topic (must exist).
     * @param topicId - the topic to like (must exist).
     * @param like - true to like, false to unlike.
     * @return success (even if it was a no-op), failure if the person or topic
     * does not exist and fatal in case of db errors.
     * 
     * Difficulty: **
     * Used by: /likeTopic/:id (LikeTopicHandler)
     * and      /unlikeTopic/:id (UnlikeTopicHandler)
     */
    public Result likeTopic(String username, long topicId,
                            boolean like);
    
    /**
     * Like or unlike a post. Liking a post that you have already liked
     * (or unliking a post you haven't liked) is a no-op, not an error.
     * @param username - the person liking/unliking the post. Must exist.
     * @param topicId - the topic with the post to (un)like. Must exist.
     * @param post - the index of the post to (un)like. Must exist.
     * @param like - true to like, false to unlike.
     * @return failure if the person or post referenced to not exist,
     * success if the (un)like succeeded, fatal in case of other errors.
     * 
     * Difficulty: ***
     * Used by: /likePost/:topic/:number/:like (LikePostHandler)
     */
    public Result likePost(String username,
                           long topicId, int post,
                           boolean like);
    
    /**
     * Get all people who have liked a particular topic, ordered by name
     * alphabetically.
     * @param topicId The topic id. Must exist.
     * @return Success (even if the list is empty) if the topic exists,
     * failure if it does not, fatal in case of database errors.
     * 
     * Difficulty: **
     * Not used in web interface.
     */
    public Result<List<PersonView>> getLikers(long topicId);
    
    /**
     * Get the detailed view of a topic.
     * method, as it does not require paging.
     * @param topicId - the topic to get.
     * @return The topic view if one exists with the given id,
     * otherwise failure (or fatal on database errors). 
     * 
     * Difficulty: ***
     * Used by: /topic/:id (TopicHandler)
     */
    public Result<TopicView> getTopic(long topicId);

    /*
     * B.2 "Stuff with lots of joins"
     *
     * These methods only count if you implement them WITHOUT doing SQL in a
     * loop. You are allowed more than one query though.
     * 
     * Implement at most ONE of these methods for extra credit and only if you
     * have already implemented "likes".
     */

    /**
     * Get the "main page" containing a list of forums ordered alphabetically
     * by title. Advanced version.
     * @return the list of all forums.
     * 
     * Difficulty: ***
     * Used by: /forums2 (AdvancedForumsHandler)
     */
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums();

    /**
     * Get an AdvancedPersonView for the person with the given username.
     * @param username - the username to search for, cannot be empty.
     * @return If a person with the given username exists, a fully populated
     * AdvancedPersonView. Otherwise, failure (or fatal on a database error).
     * 
     * Difficulty: ***
     * Used by: /person2/:id (AdvancedPersonHandler)
     */
    public Result<AdvancedPersonView> getAdvancedPersonView(String username);

    /**
     * Get the detailed view of a single forum, advanced version.
     * @param id - the id of the forum to get.
     * @return A view of this forum if it exists, otherwise failure.
     * 
     * Difficulty: ***
     * Used by: /forum2/:id (AdvancedForumHandler)
     */
    public Result<AdvancedForumView> getAdvancedForum(long id);
}
