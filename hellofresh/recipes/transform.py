def has_chili_2(tok_ings):
    def one_change(first, second):
        if first == second:
            return True
        if len(first) == len(second):
            count = 0
            for i in range(len(first)):
                if first[i] != second[i]:
                    count += 1
                    if count > 1:
                        return False
            return True
        if len(second) > len(first):
            first, second = second, first
        gap = 0
        if (len(first) == len(second)+1):
            for i in range(len(second)):
                if first[i+gap] != second[i]:
                   gap += 1
                   if (gap > 1):
                       return False
            return True
        return False
    for tok in tok_ings:
        if (one_change(tok, "chili") or one_change(tok, "chilies") ):
            return True
    return False

def total_time_2(cook_time, prep_time):
    def get_minutes(str):
        if str:
            tot_amount = 0
            ptime_str = str[2:]
            if "H" in ptime_str:
                tot_amount = int(ptime_str[:ptime_str.index("H")])*60
                ptime_str = ptime_str[ptime_str.index("H")+1:]
            if "M" in ptime_str:
                tot_amount += int(ptime_str[:ptime_str.index("M")])
            return tot_amount
        else:
            return None
    ckm, ptm  = get_minutes(cook_time), get_minutes(prep_time)

    return ( ckm + ptm ) if ckm is not None and ptm is not None else None

def difficulty_2(total_time):
    if total_time is None:
        return "Unknown"
    elif int(total_time) < 30:
        return "Easy"
    elif int(total_time) < 60:
        return "Medium"
    else:
        return "Hard"


