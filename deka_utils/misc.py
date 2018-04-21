def split_to_batches(big_list, items_per_batch):
    """
    https://stackoverflow.com/questions/9671224/split-a-python-list-into-other-sublists-i-e-smaller-lists

    Yield successive items_per_batch-sized chunks from big_list.
    """
    batches = [big_list[x:x + items_per_batch] for x in range(0, len(big_list), items_per_batch)]
    assert (sum([len(batch) for batch in batches])) == len(big_list)
    return batches


