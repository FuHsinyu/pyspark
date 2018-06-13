import re
import datetime
from pyspark.sql import Row
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import desc
import ast
from pyspark.sql.types import LongType
import emoji


def count_coments_by_user():
    # COunt comments of user
    input_file = "userWallComments.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_coms = spark.read.parquet(file_name)
    user_wall_coms.distinct().groupBy("from_id").count().sort(
        desc("count")).show()


def count_allposts_by_user():
    # COunt posts of user
    input_file = "userWallPosts.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_coms = spark.read.parquet(file_name)
    print("ALL POSTS")
    all_posts = user_wall_coms.distinct().groupBy("owner_id")
    all_posts.count().sort(desc("count")).show()

    print("ORIGINAL POSTS")
    original_posts = user_wall_coms.distinct().groupBy(
        "owner_id", "is_reposted").count()
    original_posts.filter(user_wall_coms.is_reposted ==
                          False).sort(desc("count")).show()

    print("REPOSTS POSTS")
    reposted_posts = user_wall_coms.distinct().groupBy(
        "owner_id", "is_reposted").count()
    reposted_posts.filter(user_wall_coms.is_reposted ==
                          True).sort(desc("count")).show()


def count_likes_by_user():
    input_file = "userWallLikes.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_likes = spark.read.parquet(file_name)
    count_user_likes = user_wall_likes.groupBy("likerId").count()
    count_user_likes = count_user_likes.sort(desc("count")).show()


def count_for_userWallProfiles():
    # VIDEOS AUDIOS FOLLOWERS
    input_file = "userWallProfiles.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_profiles = spark.read.parquet(file_name)
    user_wall_profiles.select(user_wall_profiles.id, "counters").rdd.map(
        lambda kv: ast.literal_eval(kv[1])).toDF().select("videos", "audios", "followers", "friends", "photos", "subscriptions").withColumnRenamed("subscriptions", "groups").show()


def get_incoming_comms_stat():
    input_file = "userWallComments.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_comms = spark.read.parquet(file_name)

    count_wall_comms = user_wall_comms.distinct().groupBy("post_id").count()
    count_wall_comms.describe("count").show()


def get_incoming_likes_stat():
    input_file = "userWallLikes.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_likes = spark.read.parquet(file_name)
    count_wall_likes = user_wall_likes.distinct().groupBy("itemId").count()
    count_wall_likes.describe("count").show()


def count_geo_tag():
    input_file = "userWallPosts.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_posts = spark.read.parquet(file_name)

    dropped_posts = user_wall_posts.select("geo").na.drop()
    dropped_posts.show()
    print("counts of geo tagged:", dropped_posts.count())


def count_open_closed_groups():
    input_file = "groupsProfiles.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    groups_profiles = spark.read.parquet(file_name)
    input_file = "userGroupsSubs.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    groups_subs = spark.read.parquet(file_name)
    groups_profiles = groups_profiles.select("key", "is_closed")
    open_closed_count = groups_subs.join(groups_profiles, groups_profiles.key ==
                                         groups_subs.group, "inner").groupBy("user", "is_closed").count()
    print("open and closed group count:")
    open_closed_count.sort(desc("user")).show()


def count_reposts_from_sub_nonsub():
    extract_id = F.udf(lambda x: x.orig_from_id)
    combine_to_key = F.udf(lambda x, y: str(x)+"_"+str(y))
    input_file = "userGroupsSubs.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_groups_subs = spark.read.parquet(file_name)
    input_file = "userWallPosts.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_posts = spark.read.parquet(file_name)
    refined_reposts = user_wall_posts.filter(user_wall_posts.is_reposted == True).select(
        "from_id", extract_id(user_wall_posts.repost_info).alias("orig_from_id"))
    sec_refined_reposts = refined_reposts.withColumn(
        "new_orig_id", refined_reposts.orig_from_id.cast(LongType()))
    ready_reposts = sec_refined_reposts.filter(sec_refined_reposts.new_orig_id < 0).withColumn(
        "key", combine_to_key(sec_refined_reposts.from_id, sec_refined_reposts.orig_from_id))
    count_from_sub = ready_reposts.join(
        user_groups_subs, ready_reposts.key == user_groups_subs.key, "inner").select(ready_reposts.key).count()
    print("count from subbed group:", count_from_sub)
    print("count from non-subbed group:", ready_reposts.count()-count_from_sub)


def count_del_user_in_fris_fols():
    input_file = "friendsProfiles.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    friends_pros = spark.read.parquet(file_name)
    input_file = "followerProfiles.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    followers_pros = spark.read.parquet(file_name)

    del_fri_count = friends_pros.distinct().filter(
        friends_pros.deactivated == "deleted").count()
    print("delted friends count:", del_fri_count)

    del_fol_count = followers_pros.distinct().filter(followers_pros.deactivated ==
                                                     "deleted").count()
    print("deleted follower count:", del_fol_count)


def like_from_folORfri_per_post():
    combine_pro_fol = F.udf(lambda x, y: str(x)+"_"+str(y))
    input_file = "userWallLikes.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_likes = spark.read.parquet(file_name)
    input_file = "followers.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    followers = spark.read.parquet(file_name)
    input_file = "friends.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    friends = spark.read.parquet(file_name)

    # LIKE PER POST
    ready_likes = user_wall_likes.filter(user_wall_likes.itemType == "post").select("itemId", "ownerId", "likerId").withColumn("key_pro_fol", combine_pro_fol(
        user_wall_likes.ownerId, user_wall_likes.likerId)).withColumn("key_fol_pro", combine_pro_fol(user_wall_likes.likerId, user_wall_likes.ownerId))
    item_like_friend = ready_likes.join(
        friends, ready_likes.key_fol_pro == friends.key, "inner").groupBy(ready_likes.itemId).count().withColumnRenamed("count", "like_fri_count")
    item_like_follower = ready_likes.join(followers, ready_likes.key_pro_fol == followers.key, "inner").groupBy(
        ready_likes.itemId).count().withColumnRenamed("count", "like_fol_count")
    like_final_res = item_like_follower.join(
        item_like_friend, "itemId", "outer").na.fill(0)
    print("likes per post stats:")
    like_final_res.describe("like_fol_count", "like_fri_count").show()


def like_from_folORfri_per_user():
    combine_pro_fol = F.udf(lambda x, y: str(x)+"_"+str(y))
    input_file = "userWallLikes.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_likes = spark.read.parquet(file_name)
    input_file = "followers.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    followers = spark.read.parquet(file_name)
    input_file = "friends.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    friends = spark.read.parquet(file_name)
    # LIKE PER USER
    ready_likes = user_wall_likes.select("ownerId", "likerId").withColumn("key_pro_fol", combine_pro_fol(
        user_wall_likes.ownerId, user_wall_likes.likerId)).withColumn("key_fol_pro", combine_pro_fol(user_wall_likes.likerId, user_wall_likes.ownerId))
    item_like_friend = ready_likes.join(
        friends, ready_likes.key_fol_pro == friends.key, "inner").groupBy(ready_likes.ownerId).count().withColumnRenamed("count", "like_fri_count")
    item_like_follower = ready_likes.join(followers, ready_likes.key_pro_fol == followers.key, "inner").groupBy(
        ready_likes.ownerId).count().withColumnRenamed("count", "like_fol_count")
    item_like_follower.show()
    item_like_friend.show()
    like_final_res = item_like_follower.join(
        item_like_friend, "ownerId", "outer").na.fill(0)
    print("likes per user stats:")
    like_final_res.describe("like_fol_count", "like_fri_count").show()


def comm_from_folORfri_per_post():
    combine_pro_fol = F.udf(lambda x, y: str(x)+"_"+str(y))
    input_file = "userWallComments.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_comms = spark.read.parquet(file_name)
    input_file = "followers.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    followers = spark.read.parquet(file_name)
    input_file = "friends.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    friends = spark.read.parquet(file_name)

    # COMMENT PER POST
    ready_comms = user_wall_comms.select("post_id", "post_owner", "from_id").withColumn("key_pro_fol", combine_pro_fol(
        user_wall_comms.post_owner, user_wall_comms.from_id)).withColumn("key_fol_pro", combine_pro_fol(user_wall_comms.from_id, user_wall_comms.post_owner))
    item_comm_friend = ready_comms.join(friends, ready_comms.key_fol_pro == friends.key, "inner").groupBy(
        ready_comms.post_id).count().withColumnRenamed("count", "comm_fri_count")
    item_comm_follower = ready_comms.join(followers, ready_comms.key_pro_fol == followers.key, "inner").groupBy(
        ready_comms.post_id).count().withColumnRenamed("count", "comm_fol_count")
    comm_final_res = item_comm_follower.join(
        item_comm_friend, "post_id", "outer").na.fill(0)
    print("comments per post stats:")
    comm_final_res.describe("comm_fol_count", "comm_fri_count").show()


def comm_from_folORfri_per_user():
    combine_pro_fol = F.udf(lambda x, y: str(x)+"_"+str(y))
    input_file = "userWallComments.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_comms = spark.read.parquet(file_name)
    input_file = "followers.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    followers = spark.read.parquet(file_name)
    input_file = "friends.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    friends = spark.read.parquet(file_name)
    # COMMENT PER USER
    ready_comms = user_wall_comms.select("post_owner", "from_id").withColumn("key_pro_fol", combine_pro_fol(
        user_wall_comms.post_owner, user_wall_comms.from_id)).withColumn("key_fol_pro", combine_pro_fol(user_wall_comms.from_id, user_wall_comms.post_owner))
    item_comm_friend = ready_comms.join(friends, ready_comms.key_fol_pro == friends.key, "inner").groupBy(
        ready_comms.post_owner).count().withColumnRenamed("count", "comm_fri_count")
    item_comm_follower = ready_comms.join(followers, ready_comms.key_pro_fol == followers.key, "inner").groupBy(
        ready_comms.post_owner).count().withColumnRenamed("count", "comm_fol_count")
    comm_final_res = item_comm_follower.join(
        item_comm_friend, "post_owner", "outer").na.fill(0)
    print("comments per user stats:")
    comm_final_res.describe("comm_fol_count", "comm_fri_count").show()


def extract_emojis(str):
    return ''.join(c for c in str if c in emoji.UNICODE_EMOJI)


def count_neg_emo(str):
    count = 0
    negative_emojis = '😒😞😟😠😡😣☹️🙁😕😔😖😫😩😤😮😦😯😰😨😱😧😭😢😵😥😲😪😓😷🤕🤒🙄💔😳'
    for emo in str:
        if emo in negative_emojis:
            count += 1
    return count


def count_pos_emo(str):
    count = 0
    positive_emojis = "😀😄😊😌😚🤓😅🙂😍😜😎😆🙃😘😝🤗😂😇☺️😗😛😏😃😉😋😙😁🤑💖💙♥🙈👧👩😈"
    for emo in str:
        if emo in positive_emojis:
            count += 1
    return count


def count_netural_emo(str):
    count = 0
    netural_emojis = "🤐😴😑😐😶🤔😬👧👩"
    for emo in str:
        if emo in netural_emojis:
            count += 1
    return count


def count_neg_pos_net_emojis_from_postcomm():
    extract_emojis_udf = F.udf(lambda x: extract_emojis(x))
    count_neg_emo_udf = F.udf(lambda x: count_neg_emo(x), LongType())
    count_pos_emo_udf = F.udf(lambda x: count_pos_emo(x), LongType())
    count_netural_emo_udf = F.udf(lambda x: count_netural_emo(x), LongType())
    # EMOJI COUNT FROM POST
    input_file = "userWallPosts.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_posts = spark.read.parquet(file_name)

    ready_emoji_text = user_wall_posts.select("text").filter(user_wall_posts.text != '').na.drop(
    ).select(extract_emojis_udf(user_wall_posts.text).alias('extracted_emoji'))
    emoji_count_table = ready_emoji_text.filter(ready_emoji_text.extracted_emoji != '').select(
        "extracted_emoji", count_neg_emo_udf(ready_emoji_text.extracted_emoji).alias("negative_count"), count_pos_emo_udf(ready_emoji_text.extracted_emoji).alias("positive_count"), count_netural_emo_udf(ready_emoji_text.extracted_emoji).alias("netural_count"))
    print("Emojis count from post")
    emoji_count_table.groupBy().sum(
        'negative_count', 'positive_count', 'netural_count').show()

    # EMOJI COUNT FROM COMMENTS
    input_file = "userWallComments.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_comms = spark.read.parquet(file_name)

    ready_emoji_text = user_wall_comms.select("text").filter(user_wall_comms.text != '').na.drop(
    ).select(extract_emojis_udf(user_wall_comms.text).alias('extracted_emoji'))
    emoji_count_table = ready_emoji_text.filter(ready_emoji_text.extracted_emoji != '').select(
        "extracted_emoji", count_neg_emo_udf(ready_emoji_text.extracted_emoji).alias("negative_count"), count_pos_emo_udf(ready_emoji_text.extracted_emoji).alias("positive_count"), count_netural_emo_udf(ready_emoji_text.extracted_emoji).alias("netural_count"))
    print("Emojis count from comments")
    emoji_count_table.groupBy().sum(
        'negative_count', 'positive_count', 'netural_count').show()


# Initialization
global data_saving_dir
data_saving_dir = "/media/hsin/16FC55FBFC55D619/linux-ubuntu-shared-data/small_data/"
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("VK_ANALYSIS") \
    .getOrCreate() \

# BASIC LEVEL TASKS
# count_coments_by_user()
# count_allposts_by_user()
# count_likes_by_user()
# count_for_userWallProfiles()
# get_incoming_comms_stat()
# get_incoming_likes_stat()
# count_geo_tag()
# count_open_closed_groups()

# MEDIUM LEVEL TAKS
# count_reposts_from_sub_nonsub()
# count_del_user_in_fris_fols()
# like_from_folORfri_per_post()
# like_from_folORfri_per_user()
# comm_from_folORfri_per_post()
# comm_from_folORfri_per_user()
# count_neg_pos_net_emojis_from_postcomm()
