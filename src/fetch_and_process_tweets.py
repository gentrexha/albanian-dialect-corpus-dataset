from git import Repo
from pathlib import Path
from tqdm import tqdm
import pandas as pd


def clone_repository(repo_url: str, target_path: Path):
    """
    Clone a git repository to the specified target path.
    """
    Repo.clone_from(repo_url, target_path)


def process_tweets(data_folder: Path) -> pd.DataFrame:
    """
    Process tweets from data folders and create a DataFrame.
    """
    data = []

    for region_folder in tqdm(data_folder.iterdir(), desc="Processing regions"):
        if region_folder.is_dir():
            region = region_folder.name
            for user_file in tqdm(region_folder.iterdir(), desc=f"Processing {region}"):
                with user_file.open("r", encoding="utf-8") as file:
                    tweets = file.readlines()
                    user = user_file.stem
                    for tweet in tweets:
                        data.append(
                            {
                                "tweet": tweet.strip(),
                                "region": region,
                                "user": user,
                            }
                        )
    return pd.DataFrame(data)


def main():
    repo_url = "https://github.com/rexshijaku/albanian-dialect-corpus.git"
    target_path = project_dir / "data" / "raw" / "albanian-dialect-corpus"

    # Clone the repository if not already present
    if not target_path.exists():
        print("Cloning repository...")
        clone_repository(repo_url, target_path)

    data_dir = target_path / "data"
    df = process_tweets(data_dir)

    # Save DataFrame to CSV
    df.to_csv(project_dir / "data" / "processed" / "tweets.csv", index=False)
    print("Data processing complete. CSV saved.")


if __name__ == "__main__":
    project_dir = Path(__file__).resolve().parents[1]
    main()
