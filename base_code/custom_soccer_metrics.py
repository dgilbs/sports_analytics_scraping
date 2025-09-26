def creative_passing_index(row):
    key_pass = row['key_passes']/row['minutes'] * 90
    asst = row['assists']/row['minutes'] * 90
    xag = row['xag']/row['minutes'] * 90 
    num = key_pass * 0.4 + asst * 2 + xag * 3
    return num

def convert_to_per_90(metric_col, minutes_col):
    calc = metric_col/minutes_col * 90
    return round(calc, 4)

def carry_retention_rate(row):
    carries = row['carries']
    miscontrol = row['carries_miscontrolled']
    dispossessed = row['carries_dispossessed']
    diff = carries - miscontrol - dispossessed 
    ratio = diff/carries
    return round(ratio * 100, 4) 

def touch_efficiency(row):
    prog_p = row['progressive_passes_recieved']
    prog_c = row['progressive_carries']
    touches = row['touches']
    final = (prog_p + prog_c)/touches
    return round(final * 100, 4)

def pressing_ratio(row):
    att = row['tackles_att_third']
    defend = row['tackles_def_third']
    num = att/defend
    return round(num, 4)

def tackling_score(row):
    #tacklingScore = tackles_won_90 * (tackle_success_rate / 100) * (1 + (tackles_att / 10))
    try:
        tackles_per_90 = convert_to_per_90(row['tackles_won'], row['minutes'])
        tackle_success_rate = round(row['tackles_won']/row['tackles_att'], 4)
        tackles_att_score = 1 + (row['tackles_att']/10)
        final = tackles_per_90 * tackle_success_rate * tackles_att_score
    except:
        final = 0
    return round(final, 4)