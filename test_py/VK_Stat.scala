
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.{Level, Logger}
import com.vdurmont.emoji._
import scala.collection.JavaConverters._

// TASK: find top 20 commented, liked, reposted posts
// comments
// followers
// posts
// likes
// { "key" : "-68471405_post_8587479_180000112" , "itemType" : "post" , "itemId" : 8587479 , "ownerId" : -68471405 , "likerId" : 180000112 , "collected_timestamp" : "1521552468"}

object VK_Stat {
  val SPARK_ADRESS = "local[*]"
  val dir = s"../BigDataCourse/data/bgdata_small"
  val UPDATE_FRIENDS_EDGES = false

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("VK Stat")
      .master(SPARK_ADRESS)
      .getOrCreate()
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    import spark.implicits._
    println("Hello, this is VK stats:")
    val start_time = System.nanoTime

    //    DATA for Task 1
    val df_userWallPosts = spark.read.parquet(s"$dir/userWallPosts.parquet")
    print("Wall Posts: ")
    df_userWallPosts.printSchema()
    val df_likes = spark.read.parquet(s"$dir/likes.parquet")
    print("Likes: ")
    df_likes.printSchema()
    val df_userWallComments = spark.read.parquet(s"$dir/userWallComments.parquet")
    print("Wall Comments: ")
    df_userWallComments.printSchema()
    //    DATA for Task 2
    val df_followers = spark.read.parquet(s"$dir/followers.parquet")
      .withColumn("profile", col("profile").cast(LongType))
      .withColumn("follower", col("follower").cast(LongType))
      .drop(col("key"))
      .withColumn("from_follower", lit(1))
    print("Followers: ")
    df_followers.printSchema()

    val df_userWallLikes = spark.read.parquet(s"$dir/userWallLikes.parquet")
    println("User Wall Likes: ")
    df_userWallLikes.printSchema()
    //    FRIENDS EDGES:
    if (UPDATE_FRIENDS_EDGES) {
      val df_friend = spark.read.parquet(s"$dir/friends.parquet")
        .withColumn("profile", col("profile").cast(LongType))
        .withColumn("follower", col("follower").cast(LongType))
        .drop(col("key"))
        .dropDuplicates(Seq("profile", "follower"))
        .cache()
      //    TODO saving friends edges to parquet
      df_friend.count()
      //    Saving real all edges between friends to parquet
      df_friend
        .union(
          df_friend
            .withColumn("follower1", col("profile"))
            .withColumn("profile", col("follower"))
            .withColumn("follower", col("follower1"))
            .drop(col("follower1"))
        )
        .dropDuplicates(Seq("profile", "follower"))
        .write.parquet(s"$dir/friends_edges.parquet")
    }
    val df_friends = spark.read.parquet(s"$dir/friends_edges.parquet")
      .withColumn("from_friend", lit(1))
    print("Friends: ")
    df_friends.printSchema()
    val df_userGroupsSubs = spark.read.parquet(s"$dir/userGroupsSubs.parquet")
      .withColumn("group", col("group").cast(LongType))
      .withColumn("user", col("user").cast(LongType))
      .withColumn("is_subscribed", lit(1))
      .cache()
    print("Groups: ")
    df_userGroupsSubs.printSchema()
    val df_groups_info = spark.read.parquet(s"$dir/groupsProfiles.parquet")
    print("Groups: ")
    df_groups_info.printSchema()

    //    Basic level:  \\
    //    1) count of comments, posts (all), original posts, reposts and likes made by user
    //    2) count of friends, groups, followers
    //    3) count of videos, audios, photos, gifts
    //    4) count of "incoming" (made by other users) comments, max and mean "incoming" comments per post
    //    5) count of "incoming" likes, max and mean "incoming" likes per post
    //    6) count of geo tagged posts
    //    7) count of open / closed (e.g. private) groups a user participates in

    ////   TASK 1 \\\\
    //    1) count of comments, posts (all), original posts, reposts and likes made by user
    val t1 = df_userWallPosts.groupBy("from_id").count().select($"from_id".alias("id"), $"count".alias("posts_all"))
      .join(df_userWallPosts
        .filter(col("is_reposted") === false)
        .groupBy("from_id").count()
        .select($"from_id".alias("id"), $"count".alias("posts_original")), Seq("id"), joinType = "outer")
      .join(df_userWallPosts
        .filter(col("is_reposted") === true)
        .groupBy("from_id").count().select($"from_id".alias("id"), $"count".alias("reposts")), Seq("id"), joinType = "outer")
            .join(df_likes.groupBy('likerId).count().select($"likerId".alias("id"), $"count".alias("likes")), Seq("id"), joinType = "outer")
            .join(df_userWallComments.groupBy('from_id).count().select($"from_id".alias("id"), $"count".alias("comments")), Seq("id"), joinType = "outer")
      .withColumn(
      "Task1",
      struct(
        col("posts_all"), col("posts_original"), col("reposts") //, col("comments"), col("likes")
      )
    )
      .persist(MEMORY_AND_DISK_SER)
    ////   TASK 2 \\\\
    //    2) count of friends, groups, followers
    val t2 = df_friends.groupBy("profile").count().select($"profile".alias("id"), $"count".alias("friends"))
      .join(df_followers.groupBy("profile").count().select($"profile".alias("id"), $"count".alias("followers")), Seq("id"), joinType = "outer")
      .join(df_userGroupsSubs.groupBy("user").count().select($"user".alias("id"), $"count".alias("groups")), Seq("id"), joinType = "outer")
      .withColumn(
        "Task2",
        struct(
          col("friends"), col("groups"), col("followers")
        )
      )
      .cache()
    ////   TASK 3 \\\\ TODO to ask about another sources of info
    //    3) count of videos, audios, photos, gifts
    val t3 = spark.read.parquet(s"$dir/friendsProfiles.parquet").filter(col("counters").isNotNull)
      .select("id", "counters").dropDuplicates()
      .union(
        spark.read.parquet(s"$dir/followerProfiles.parquet").filter(col("counters").isNotNull)
          .select("id", "counters").dropDuplicates()
      )
      .select(col("id"), col("counters")("videos").as("videos"), col("counters")("audios").as("audios"), col("counters")("photos").as("photos"), col("counters")("gifts").as("gifts"))
      .withColumn(
        "Task3",
        struct(
          col("videos"), col("audios"), col("photos"), col("gifts")
        )
      )
      .cache()

    ////   TASK 4 \\\\
    //    4) count of "incoming" (made by other users) comments, max and mean "incoming" comments per post
    val t4 = df_userWallComments
      .filter(col("post_owner") =!= col("from_id"))
      .groupBy("post_owner", "post_id").count()
      .groupBy(col("post_owner").alias("id")).agg(sum("count").alias("comment_cnt_incoming"),
      max("count").alias("comment_max_incoming"),
      mean("count").alias("comment_mean_incoming"))
      .withColumn(
        "Task4",
        struct(
          col("comment_cnt_incoming").alias("cnt_incoming"), col("comment_max_incoming").alias("max_incoming"), col("comment_mean_incoming").alias("mean_incoming")
        )
      )
      .cache()
    ////   TASK 5 \\\\
    //    5) count of "incoming" likes, max and mean "incoming" likes per post
    val t5 = df_userWallLikes
      .filter((col("itemType") === "post") && col("ownerId") =!= col("likerId")).groupBy("ownerId", "itemId").count()
      .groupBy(col("ownerId").alias("id")).agg(sum("count").alias("like_cnt_incoming"),
      max("count").alias("like_max_incoming"),
      mean("count").alias("like_mean_incoming"))
      .withColumn(
        "Task5",
        struct(
          col("like_cnt_incoming").alias("cnt_incoming"), col("like_max_incoming").alias("max_incoming"), col("like_mean_incoming").alias("mean_incoming")
        )
      )
      .cache()
    ////   TASK 6 \\\\
    //    6) count of geo tagged posts
    val t6 = df_userWallPosts
      .filter(col("geo").isNotNull).groupBy("from_id").count()
      .select(col("from_id").alias("id"), col("count").alias("geo_tagged_posts"))
      .withColumn(
        "Task6",
        struct(
          col("geo_tagged_posts")
        )
      )
      .cache()
    ////   TASK 7 \\\\
    //    7) count of open / closed (e.g. private) groups a user participates in
    val t7 = df_userGroupsSubs
      .select(abs(col("group")).alias("id"), col("user"))
      .join(df_groups_info.filter(col("type") === "group").select(col("id"), col("is_closed")), Seq("id"), joinType = "inner")
      .groupBy("user")
      .agg(sum("is_closed").alias("closed_groups"),
        (count("is_closed") - sum("is_closed")).alias("open_groups"),
        (sum("is_closed") / count("is_closed")).alias("groups_ratio"))
      .withColumn(
        "Task7",
        struct(
          col("closed_groups"), col("open_groups"), col("groups_ratio")
        )
      )
      .cache()

    //    Medium level:   \\
    //    1) count of reposts from subscribed and not-subscribed groups
    //    2) count of deleted users in friends and followers
    //    3) aggregate (e.g. count, max, mean) characteristics for comments and likes (separtely) made by (a) friends and (b) followers per post
    //    4) aggregate (e.g. count, max, mean) characteristics for comments and likes (separtely) made by (a) friends and (b) followers per user
    //    5) find emoji (separately, count of: all, negative, positive, others) in (a) user's posts (b) user's comments

    ////   TASK 2.1 \\\\
    //    1) count of reposts from subscribed and not-subscribed groups TODO current implementation filters only groups for repost entity
    val t8 = df_userWallPosts.filter(col("is_reposted") === true)
      .select(col("from_id").alias("user"), col("repost_info.orig_owner_id").alias("group"))
      .join(df_groups_info.filter(col("type") === "group").select(col("key").alias("group")), Seq("group"))
      .dropDuplicates()
      .join(df_userGroupsSubs, Seq("group", "user"), "left")
      .na.fill(0, Seq("is_subscribed"))
      .groupBy("user")
      .agg(sum("is_subscribed").alias("subscribed"),
        (count("is_subscribed") - sum("is_subscribed")).alias("not_subscribed"),
        (sum("is_subscribed") / count("is_subscribed")).alias("subscribe_ratio"))
      .orderBy("user")
      .cache()

    ////   TASK 2.2 \\\\
    //    2) count of deleted users in friends and followers
    val df_Profiles = spark.read.parquet(s"$dir/friendsProfiles.parquet")
      .filter(col("deactivated") === "deleted").select(col("id").alias("follower"))
      .union(
        spark.read.parquet(s"$dir/followerProfiles.parquet")
          .filter(col("deactivated") === "deleted").select(col("id").alias("follower"))
      )
      .dropDuplicates()
      .cache()
    df_Profiles.count() // for caching
    val t9 = df_friends
      .join(df_Profiles, Seq("follower"), "inner")
      .groupBy(col("profile").alias("id"))
      .agg(count("*").alias("cnt_deleted_friends"))
      .join(df_followers
        .join(df_Profiles, Seq("follower"), "inner")
        .groupBy(col("profile").alias("id"))
        .agg(count("*").alias("cnt_deleted_followers")),
        Seq("id"), "outer")
      .withColumn(
        "Task9",
        struct(
          col("cnt_deleted_friends"), col("cnt_deleted_followers")
        )
      )

    ////   TASK 2.3-2.4 \\\\
    // Working with comments data:
    val tmp1 = df_userWallComments
      .select(col("post_owner_id"), col("id"), col("from_id"))
      .join(df_followers,
        df_userWallComments("post_owner_id") === df_followers("profile")
          && df_userWallComments("from_id") === df_followers("follower"), "left")
      .drop("profile", "follower")
    val tmp_comments = tmp1
      .join(df_friends,
        tmp1("post_owner_id") === df_friends("profile")
          && tmp1("from_id") === df_friends("follower"), "left")
      .na.fill(0, Seq("from_follower", "from_friend"))
      .select(col("post_owner_id"), col("id"), col("from_id"),
        col("from_follower").alias("comment_from_follower"), col("from_friend").alias("comment_from_friend")
      )
      .cache()
    //    Working with Likes data:
    val tmp2 = df_userWallLikes
      .join(df_followers,
        df_userWallLikes("ownerId") === df_followers("profile")
          && df_userWallLikes("likerId") === df_followers("follower"), "left")
      .select(col("ownerId").alias("post_owner_id"), col("itemId").alias("id"), col("likerId").alias("from_id"),
        col("from_follower"))
    val tmp_likes = tmp2
      .join(df_friends,
        tmp2("post_owner_id") === df_friends("profile")
          && tmp2("from_id") === df_friends("follower"), "left")
      .na.fill(0, Seq("from_follower", "from_friend"))
      .select(col("post_owner_id"), col("id"), col("from_id"),
        col("from_follower").alias("like_from_follower"), col("from_friend").alias("like_from_friend")
      )
      .cache()
    //    RDD with information about post, id, and boolean flags for friend/follower action
    val tmp = tmp_comments
      .join(tmp_likes, Seq("post_owner_id", "id", "from_id"), "outer")
      .dropDuplicates()
      .na.fill(0)
      .cache()
    tmp.count()
    //    tmp.write.parquet(s"$dir/post_follower_friend.parquet")
    ////   TASK 2.3\\\\
    val t10 = tmp
      .groupBy(col("post_owner_id"), col("id"))
      .agg(sum("comment_from_follower").alias("comment_from_follower_post"),
        sum("comment_from_friend").alias("comment_from_friend_post"),
        sum("like_from_follower").alias("like_from_follower_post"),
        sum("like_from_friend").alias("like_from_friend_post"))
      .groupBy(col("post_owner_id").alias("id"))
      .agg(sum("comment_from_follower_post").alias("cnt_comment_followers"),
        sum("comment_from_friend_post").alias("cnt_comment_friends"),
        max("comment_from_follower_post").alias("max_comment_followers"),
        max("comment_from_friend_post").alias("max_comment_friends"),
        mean("comment_from_follower_post").alias("mean_comment_followers"),
        mean("comment_from_friend_post").alias("mean_comment_friends"),
        //          Likes aggregation
        sum("like_from_follower_post").alias("cnt_like_followers"),
        sum("like_from_friend_post").alias("cnt_like_friends"),
        max("like_from_follower_post").alias("max_like_followers"),
        max("like_from_friend_post").alias("max_like_friends"),
        mean("like_from_follower_post").alias("mean_like_followers"),
        mean("like_from_friend_post").alias("mean_like_friends"))
      .orderBy("id")
      .withColumn(
        "Comments_perPost",
        struct(
          col("cnt_comment_followers"),
          col("cnt_comment_friends"),
          col("max_comment_followers"),
          col("max_comment_friends"),
          col("mean_comment_followers"),
          col("mean_comment_friends")
        )
      )
      .withColumn(
        "Likes_perPost",
        struct(
          col("cnt_like_followers"),
          col("cnt_like_friends"),
          col("max_like_followers"),
          col("max_like_friends"),
          col("mean_like_followers"),
          col("mean_like_friends")
        )
      )
      .cache()
    //    Call count for future caching.
    t10.count()
    ////   TASK 2.4 \\\\
    //    4) aggregate (e.g. count, max, mean) characteristics for comments and likes (separtely)
    //    made by (a) friends and (b) followers per user
    val t11 = tmp
      .groupBy(col("post_owner_id"), col("from_id"))
      .agg(sum("comment_from_follower").alias("comment_from_follower_post"),
        sum("comment_from_friend").alias("comment_from_friend_post"),
        sum("like_from_follower").alias("like_from_follower_post"),
        sum("like_from_friend").alias("like_from_friend_post"))
      .groupBy(col("post_owner_id").alias("id"))
      .agg(sum("comment_from_follower_post").alias("cnt_comment_followers"),
        sum("comment_from_friend_post").alias("cnt_comment_friends"),
        max("comment_from_follower_post").alias("max_comment_followers"),
        max("comment_from_friend_post").alias("max_comment_friends"),
        mean("comment_from_follower_post").alias("mean_comment_followers"),
        mean("comment_from_friend_post").alias("mean_comment_friends"),
        //          Likes aggregation
        sum("like_from_follower_post").alias("cnt_like_followers"),
        sum("like_from_friend_post").alias("cnt_like_friends"),
        max("like_from_follower_post").alias("max_like_followers"),
        max("like_from_friend_post").alias("max_like_friends"),
        mean("like_from_follower_post").alias("mean_like_followers"),
        mean("like_from_friend_post").alias("mean_like_friends"))
      .orderBy("id")
      .withColumn(
        "Comments_perUser",
        struct(
          col("cnt_comment_followers"),
          col("cnt_comment_friends"),
          col("max_comment_followers"),
          col("max_comment_friends"),
          col("mean_comment_followers"),
          col("mean_comment_friends")
        )
      )
      .withColumn(
        "Likes_perIser",
        struct(
          col("cnt_like_followers"),
          col("cnt_like_friends"),
          col("max_like_followers"),
          col("max_like_friends"),
          col("mean_like_followers"),
          col("mean_like_friends")
        )
      )
      .cache()
    t11.count()

    ////   TASK 2.5 \\\\
    //    5) find emoji (separately, count of: all, negative, positive, others) in (a) user's posts (b) user's comments
    ////   ParsEmojiUDF here  \\\\
    val emojiList = spark.sparkContext.broadcast(Map(
      "Positive" -> EmojiParser.extractEmojis("😀😄😊😌😚🤓😅🙂😍😜😎😆🙃😘😝🤗😂😇☺️😗😛😏😃😉😋😙😁🤑"),
      "Neutral" -> EmojiParser.extractEmojis("🤐😴😑😐😶🤔😬"),
      "Negative" -> EmojiParser.extractEmojis("😒😞😟😠😡😣☹️🙁😕😔😖😫😩😤😮😦😯😰😨😱😧😭😢😵😥😲😪😓😷🤕🤒")))
    val parseEmoji: (String) => (Int, Int, Int) = _text => {
      var inside = EmojiParser.extractEmojis(_text).asScala
      var p = 0
      var n = 0
      var neg = 0
      for (_cur <- inside) {
        if (emojiList.value("Positive").contains(_cur)) p += 1
        if (emojiList.value("Neutral").contains(_cur)) n += 1
        if (emojiList.value("Negative").contains(_cur)) neg += 1
      }
      (p, n, neg)
    }
    //    Define UDF for parsing emoji from column with text
    val parseEmojiUDF = udf(parseEmoji, StructType(
      StructField("Positive", IntegerType) ::
        StructField("Neutral", IntegerType) ::
        StructField("Negative", IntegerType) :: Nil))

    val tmp_p = df_userWallPosts
      //      .filter(col("text").isNotNull && col("text")=!="")
      .withColumn("EmojiPost", parseEmojiUDF(col("text")))
      .select(col("id"), col("text"), col("from_id"), col("EmojiPost"))
      .groupBy(col("from_id").alias("id"))
      .agg(sum("EmojiPost.Positive").alias("EmojiPositivePost"),
        sum("EmojiPost.Neutral").alias("EmojiNeutralPost"),
        sum("EmojiPost.Negative").alias("EmojiNegativePost"))
      .withColumn(
        "cntEmoji_Post",
        struct(
          col("EmojiPositivePost").alias("Positive"),
          col("EmojiNeutralPost").alias("Neutral"),
          col("EmojiNegativePost").alias("Negative")
        )
      )
    val tmp_c = df_userWallComments
      //      .filter(col("text").isNotNull && col("text")=!="")
      .withColumn("EmojiComment", parseEmojiUDF(col("text")))
      .select(col("id"), col("text"), col("from_id"), col("EmojiComment"))
      .groupBy(col("from_id").alias("id"))
      .agg(sum("EmojiComment.Positive").alias("EmojiPositiveComment"),
        sum("EmojiComment.Neutral").alias("EmojiNeutralComment"),
        sum("EmojiComment.Negative").alias("EmojiNegativeComment"))
      .withColumn(
        "cntEmoji_Comment",
        struct(
          col("EmojiPositiveComment").alias("Positive"),
          col("EmojiNeutralComment").alias("Neutral"),
          col("EmojiNegativeComment").alias("Negative")
        )
      )
    val t12 = tmp_c.join(tmp_p, Seq("id"), "outer")
      .filter(col("id") > 0) // remove groups posts
      .cache()

    //    val df_profiles = spark.read.parquet(s"$dir/userWallProfiles.parquet")
    //    print("Profiles: ")
    //    df_profiles.printSchema()
    //
    //
    //    var RESULT = df_profiles.select(col("id")).distinct().cache()
    //    println("Start getting stats per user: ")
    //
    //    println(t1.count(), RESULT.count())
    //
    //
    //    //    println(RESULT.rdd.partitioner==likes_cnt.rdd.partitioner)
    //    println(RESULT.rdd.partitions.size)
    val duration = (System.nanoTime - start_time) / 1e9d
    println("DONE! elapsed time ", duration)
  }
}

