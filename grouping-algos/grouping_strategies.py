import pprint

def calc_aggregate(val_a, val_b, how):
    """ Does a calculation between val_a and val_b based on how
    """
    if(how == "SUM"):
        return val_a + val_b
    elif(how == "AVG"):
        return (val_a + val_b)/2
    elif(how == "MIN"):
        return min(val_a, val_b)
    elif(how == "MAX"):
        return max(val_a, val_b)
    elif(how == "MEDIAN"):
        pass
    else:
        raise NotImplementedError(f"The aggregation {how} is not implemented. Try SUM, AVG, MIN, MAX, MEDIAN")

class GroupingAlgos(object):
    def __init__(self, data, on_cols, agg_col, agg, algorithm) -> None:
        """ Accepts the data, the column(s) to group on and the aggregation to do. Then calls the specified algorithm

        Params:
            data - List of dictionaries representing rows
            on_cols - List of the column(s) to group on
            agg_col - The column to aggregate
            agg - List of aggregations to perform. Accepts: SUM, AVG, MIN, MAX, MEDIAN
            algorithm - The grouping algorithm to use. Accepts: StreamAggregate, HashAggregate, MixedAggregate, PartialAggregate
        """
        self.rows = data
        self.on = on_cols
        self.agg_col = agg_col
        self.agg = agg

        if(algorithm == "StreamAggregate"):
            self.streaming_aggregate()
        elif(algorithm == "HashAggregate"):
            self.hashing_aggregate()
        elif(algorithm == "MixedAggregate"):
            self.mixed_aggregate()
        elif(algorithm == "PartialAggregate"):
            self.partial_aggregate()

    def streaming_aggregate(self):
        sorted_rows = sorted(self.rows, key = lambda r: [r[k] for k in self.on])
        
        i = 0
        while i < len(sorted_rows):
            if(i == 0):
                agg_value = sorted_rows[0][self.agg_col]
            else:
                this_row = sorted_rows[i]
                prev_row = sorted_rows[i - 1]

                this_values = list(map(this_row.get, self.on))
                prev_values = list(map(prev_row.get, self.on))

                if(this_values == prev_values):
                    agg_value = agg_value
                else:
                    agg_value = 0
                agg_value = calc_aggregate(this_row[self.agg_col], agg_value, self.agg)
            i = i + 1
    

    def hashing_aggregate(self):
        pass

    def mixed_aggregate(self):
        pass

    def partial_aggregate(self):
        pass



    