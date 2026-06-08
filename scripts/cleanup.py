"""Safe teardown of the Hospital Data Platform stack."""
import subprocess
import sys


def main():
    print("=== Hospital Platform Cleanup ===\n")
    print("This will stop all containers and DELETE all volumes (all data will be lost).")
    confirm = input("\nType 'y' to continue: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    print("\nStopping containers and removing volumes...")
    subprocess.run(["docker", "compose", "down", "-v"])

    remove_images = input("\nAlso remove built images? (y/n): ").strip().lower()
    if remove_images == "y":
        print("Removing local images...")
        subprocess.run(["docker", "compose", "down", "--rmi", "local"])

    print("\nDone.")


if __name__ == "__main__":
    main()