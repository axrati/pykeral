def new_vs_update (arr1, arr2):
    creates = []
    updates = []
    for i in arr1:
        if i not in arr2:
            if i not in creates:
                creates.append(i)
        else:
            if i not in updates:
                updates.append(i)
    return creates,updates

def all_exist_in (arr1, arr2):
    check=0
    for i in arr1:
        if i not in arr2:
            check+=1
    return check==0