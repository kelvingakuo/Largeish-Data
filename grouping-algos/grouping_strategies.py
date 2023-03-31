import pprint
import json
import statistics
# TODO: Implement aggregating multiple columns
# TODO: Implement multiple aggregations on a single column

def calc_aggregate(vals, how):
    """ Does a calculation on a list of values based on how
    """
    if(how == "SUM"):
        return sum(vals)
    elif(how == "AVG"):
        return statistics.mean(vals)
    elif(how == "MIN"):
        return min(vals)
    elif(how == "MAX"):
        return max(vals)
    elif(how == "MEDIAN"):
        return statistics.median(vals)
    elif(how == "COUNT"):
        # TODO: Impelement COUNT
        pass
    else:
        raise NotImplementedError(f"The aggregation {how} is not implemented. Try SUM, AVG, MIN, MAX, MEDIAN")

class GroupingAlgos(object):
    def __init__(self, data, on_cols, agg_col, agg) -> None:
        """ Accepts the data, the column(s) to group on and the aggregation to do. Then calls the specified algorithm

        Params:
            data - List of dictionaries representing rows of data
            on_cols - List of the column(s) to group on
            agg_col - The column to aggregate
            agg - The aggregation to perform. Accepts: SUM, AVG, MIN, MAX, MEDIAN
        """
        self.rows = data
        self.on = on_cols
        self.agg_col = agg_col
        self.agg = agg

    def streaming_aggregate(self):
        sorted_rows = sorted(self.rows, key = lambda r: [r[k] for k in self.on])
        with open("sorteddd.json", "w") as fp:
            json.dump(sorted_rows, fp)


        i = 0
        if self.agg in ("AVG", "MEDIAN"): # For avg and median, we need to calculate on the entire list of values in the group
            cache_vals = [] 
        while i <= len(sorted_rows):
            if(i == 0):
                agg_value = sorted_rows[0][self.agg_col]
                if self.agg in ("AVG", "MEDIAN"): # Add the first value to the list to finally agg on
                    cache_vals.append(agg_value)
            elif(i == len(sorted_rows)):
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
    

    def hashing_aggregate(self):
        pass

    def mixed_aggregate(self):
        pass

    def partial_aggregate(self):
        pass



    