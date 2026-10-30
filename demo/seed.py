"""Seed the last available unit."""

from foremen.fdb_access import seed_inventory


def main() -> None:
    print(f"Seeded inv/sku-1; commit version {seed_inventory()}")


if __name__ == "__main__":
    main()
