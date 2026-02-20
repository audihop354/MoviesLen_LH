from pathlib import Path
from dagster import AssetCheckResult, asset_check
from movie_pipeline.assets.bronze import DATA_DIR, bronze_links, bronze_movies, bronze_ratings, bronze_tags

def _count_csv_rows(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8") as csv_file:
        return max(sum(1 for _ in csv_file) - 1, 0)

def _check_local_csv(file_name: str) -> AssetCheckResult:
    source_path = DATA_DIR / file_name
    if not source_path.exists():
        return AssetCheckResult(
            passed=False,
            description=f"Missing expected source file: {source_path}",
        )
    row_count = _count_csv_rows(source_path)
    file_size_bytes = source_path.stat().st_size
    passed = row_count > 0 and file_size_bytes > 0
    return AssetCheckResult(
        passed=passed,
        description=f"Validated {file_name} exists and contains data.",
        metadata={
            "row_count": row_count,
            "file_size_bytes": file_size_bytes,
            "source_path": str(source_path),
        },
    )

@asset_check(asset=bronze_movies)
def bronze_movies_has_data() -> AssetCheckResult:
    return _check_local_csv("movies.csv")

@asset_check(asset=bronze_links)
def bronze_links_has_data() -> AssetCheckResult:
    return _check_local_csv("links.csv")

@asset_check(asset=bronze_ratings)
def bronze_ratings_has_data() -> AssetCheckResult:
    return _check_local_csv("ratings.csv")

@asset_check(asset=bronze_tags)
def bronze_tags_has_data() -> AssetCheckResult:
    return _check_local_csv("tags.csv")