"""Helpers for tests."""


def generate_item(i):
    """Generate DynamoDB item.

    Args:
        i: Item identifier.

    Returns:
        A DynamoDB object of form { "pk": i, "attr1": "test", "attr2": i }.
    """
    return {"pk": str(i), "attr1": "test", "attr2": i}


def generate_items(n):
    """Generate N items with IDs from 0 to N.

    Args:
        n: Number of items to generate.

    Returns:
        List of item objects of form { "pk": i, "attr1": "test", "attr2": i }
        for i in range(n).
    """
    return [generate_item(i) for i in range(n)]
