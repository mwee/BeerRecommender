import numpy as np

from mrjob.job import MRJob
from itertools import combinations

from scipy.stats.stats import pearsonr

class BeerSimilarities(MRJob):
    def steps(self):
        "the steps in the map-reduce process"
        return [
            self.mr(mapper=self.line_mapper, reducer=self.users_items_reducer),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_reducer)
        ]

    def line_mapper(self, _, line):
        """Split reviews into their components are yield them keyed by username."""

        data = line.split(' ')
        username = data[0]
        beer_id = data[1]
        
        yield username, (beer_id, data[2:])

    def users_items_reducer(self, username, values):
        """Grab review data from the values iterator and yield the reviews."""
        yield (username, [v for v in values])

    def pair_items_mapper(self, username, values):
        """Take all combinations of beer pairs and yield them keyed by the pair of IDs.

        The value is the pair rating information. Also, we're ditching the username here,
        as it's not necessary for future steps.

        """
        # can't use a dictionary comprehension because Python 2.6 doesn't support them :(
        d = {}
        for v in values:
            d[v[0]] = v

        for combo in combinations([v[0] for v in values], 2):
            combo = tuple(sorted(combo))
            yield (combo, [d[combo[0]], d[combo[1]]])

    def calc_sim_reducer(self, key, values):
        """
        Compute the Pearson correlation for each review aspect, and yield the final information.
        """
        (beer_1_id, beer_2_id), common_ratings = key, values

        # separate input by beer
        beer_1_reviews, beer_2_reviews = [], []
        for pair in common_ratings:
            beer_1_reviews.append(pair[0][1])
            beer_2_reviews.append(pair[1][1])

        n_common = len(beer_1_reviews)

        num_aspects = len(beer_1_reviews[0])

        similarities = []
        for i in range(num_aspects):
            if n_common == 0:
                similarities.append(0.0)
            else:
                rho = pearsonr(
                    np.array([np.float32(x[i]) for x in beer_1_reviews]),
                    np.array([np.float32(x[i]) for x in beer_2_reviews])
                )[0]

                similarities.append(0.0 if np.isnan(rho) else rho)

        yield (beer_1_id, beer_2_id), (similarities, n_common)

if __name__ == '__main__':
    BeerSimilarities.run()
