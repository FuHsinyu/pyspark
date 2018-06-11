import re
import datetime
from pyspark.sql import Row
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import desc
import ast


def count_coments_by_user():
    # COunt comments of user
    input_file = "userWallComments.parquet"
    file_name = os.path.join(data_saving_dir, input_file)
    user_wall_coms = spark.read.parquet(file_name)
    user_wall_coms.distinct().groupBy("from_id").count().show()


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
    count_wall_comms = user_wall_comms.distinct().groupBy("post_owner_id").count()
    count_wall_comms.describe("count").show()


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
    open_count = groups_profiles.select("can_see_all_posts").filter(
        groups_profiles.can_see_all_posts == 1).count()
    print("opened group count:", open_count)
    closed_count = groups_profiles.select("can_see_all_posts").filter(
        groups_profiles.can_see_all_posts == 0).count()
    print("closed group count:", closed_count)


# Initialization
global data_saving_dir
data_saving_dir = "/media/hsin/16FC55FBFC55D619/linux-ubuntu-shared-data/small_data/"
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("VK_ANALYSIS") \
    .getOrCreate() \

# count_coments_by_user()
# count_allposts_by_user()
# count_likes_by_user()
# count_for_userWallProfiles()
# get_incoming_comms_stat()
# count_geo_tag()
count_open_closed_groups()
