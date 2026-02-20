import os

from dagster import Definitions

from movie_pipeline.assets.bronze import bronze_links, bronze_movies, bronze_ratings, bronze_tags
from movie_pipeline.checks.bronze_checks import (
    bronze_links_has_data,
    bronze_movies_has_data,
    bronze_ratings_has_data,
    bronze_tags_has_data,
)
from movie_pipeline.resources.s3_resource import S3Resource

defs = Definitions(
    assets=[bronze_movies, bronze_links, bronze_ratings, bronze_tags],
    resources={
        "s3": S3Resource(
            aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
            bucket=os.getenv("S3_BUCKET"),
        ),
    },
    asset_checks=[
        bronze_movies_has_data,
        bronze_links_has_data,
        bronze_ratings_has_data,
        bronze_tags_has_data,
    ],
)
