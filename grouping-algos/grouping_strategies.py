import pprint
import json
import statistics

# TODO: Implement aggregating multiple columns
# TODO: Implement multiple aggregations on a single column
# TODO: Implement DISTINCT aggregation
# TODO: Impelement COUNT aggregation
# TODO: Redo this logic to use Postgres' concepts of transition_function() and final_fuction() [https://www.timescale.com/blog/how-postgresql-aggregation-works-and-how-it-inspired-our-hyperfunctions-design-2/]

def calc_aggregate(vals, how):
    """ Does a calculation on a list of values based on how
    """
    if(how == "SUM"):
        return sum(vals)
    elif(how == "MIN"):
        return min(vals)
    elif(how == "MAX"):
        return max(vals)
    elif(how == "AVG"):
        return statistics.mean(vals) # TODO: AVG() can be executed as storing the sum so far and count items, then divide the two before yielding
    elif(how == "MEDIAN"): # TODO: Mention that you can't do medians on Postgres because agg doesn't store lists
        return statistics.median(vals)
    else:
        raise NotImplementedError(f"The aggregation {how} is not implemented. Try SUM, AVG, MIN, MAX, MEDIAN")
    
def hash_func(hash_cols):
    """ Hash function for hash aggregate. 

    Accepts a list of the column values to group on, then hashes by:
    1. Concating all the values into a single string
    2. Compute a hash using a Polynomial rolling hash function

    Params:
        - hash_cols: List of values to hash
    
    Returns:
        - hash_val: The hashed value
    """
    str_vals = ''.join(hash_cols)

    hash_val = 0
    p = 31
    m = 10**9 + 7
    poww = 1
    for i in range(len(str_vals)):
        hash_val = (hash_val + (1 + ord(str_vals[i]) - ord('a')) * poww) % m
        poww = (poww * p) % m
    
    return hash_val

class GroupingAlgos(object):
    def __init__(self, data, on_cols, agg_col, agg) -> None:
        """ Accepts the data, the column(s) to group on and the aggregation to do. Then calls the specified algorithm

        Params:
            data - List of dictionaries representing rows of data
            on_cols - List of the column(s) to group on
            agg_col - The column to aggregate
            agg - The aggregation to perform. Accepts: SUM, AVG, MIN, MAX, MEDIAN, DISTINCT
        """
        self.rows = data
        self.on = on_cols
        self.agg_col = agg_col
        self.agg = agg

    def hashing_aggregate(self):
        hash_table = {}
        disp_hash = {}
        for row in self.rows:
            col_values = list(map(row.get, self.on))
            row_hash = hash_func(col_values)
            

            if(row_hash not in hash_table):
                disp_hash[row_hash] = col_values
                if(self.agg in ("AVG", "MEDIAN")):
                    hash_table[row_hash] = [row[self.agg_col]]
                else:
                    hash_table[row_hash] = row[self.agg_col]
            else:
                if(self.agg in ("AVG", "MEDIAN")):
                    hash_table[row_hash].append(row[self.agg_col])
                else:
                    old_val = hash_table[row_hash]
                    new_val = calc_aggregate([row[self.agg_col], old_val], self.agg)
                    hash_table[row_hash] = new_val
        
        if(self.agg in ("AVG", "MEDIAN")):
            hash_table = {key: calc_aggregate(val, self.agg) for key, val in hash_table.items()}
        hash_table_disp = [{"group": disp_hash[key], f"group_{self.agg}": val} for key, val in hash_table.items()]
        return hash_table_disp

    def streaming_aggregate(self):
        sorted_rows = sorted(self.rows, key = lambda r: [r[k] for k in self.on])

        i = 0
        if self.agg in ("AVG", "MEDIAN"): # For avg and median, we need to calculate on the entire list of values in the group
            cache_vals = [] 
        while i <= len(sorted_rows):
            if(i == 0):
                agg_value = sorted_rows[0][self.agg_col]
                if self.agg in ("AVG", "MEDIAN"): # Add the first value to the list to finally agg on
                    cache_vals.append(agg_value)
            elif(i == len(sorted_rows)): # Dataset complete
                prev_row = sorted_rows[i - 1]
                prev_values = list(map(prev_row.get, self.on))
                if(self.agg in ("AVG", "MEDIAN")): # Calculate avg or median before yielding
                    agg_value = calc_aggregate(cache_vals, self.agg)

                yield {"group": prev_values, f"group_{self.agg}": agg_value}
            else:
                this_row = sorted_rows[i]
                prev_row = sorted_rows[i - 1]

                this_values = list(map(this_row.get, self.on))
                prev_values = list(map(prev_row.get, self.on))


                if(this_values == prev_values):
                    if(self.agg in ("AVG", "MEDIAN")):
                        cache_vals.append(this_row[self.agg_col]) # Add to list
                    else:
                        agg_value = calc_aggregate([this_row[self.agg_col], agg_value], self.agg) # Calculate
                elif(this_values != prev_values):
                    if(self.agg in ("AVG", "MEDIAN")):  # Calculate avg or median before yielding
                        agg_value = calc_aggregate(cache_vals, self.agg)
                        cache_vals = []
                    
                    yield {"group": prev_values, f"group_{self.agg}": agg_value}

                    agg_value = this_row[self.agg_col]
                    if self.agg in ("AVG", "MEDIAN"):
                        cache_vals.append(agg_value)                    
            i = i + 1


    def mixed_aggregate(self):
        pass

    def partial_aggregate(self):
        pass



    