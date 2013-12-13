import numpy as np

from mrjob.job import MRJob
from itertools import combinations

from scipy.stats.stats import pearsonr

class UserSimilarities(MRJob):
    def steps(self):
        return [
            self.mr(mapper=self.line_mapper, reducer=self.beer_items_reducer),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_reducer)
        ]

    def line_mapper(self, _, line):
        """Split reviews into their components and yield them keyed by beer_id."""

        data = line.split(' ')
        username = data[0]
        beer_id = data[1]
        
        yield beer_id, (username, data[2:])

    def beer_items_reducer(self, beer_id, values):
        """Grab review data from the values iterator and yield the reviews."""
        yield (beer_id, [v for v in values])

    def pair_items_mapper(self, beer_id, values):
        """Take all combinations of user pairs and yield them keyed by the pair of IDs.

        The value is the pair rating information. Also, we're ditching the beer_id here,
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
        (user_1_id, user_2_id), common_ratings = key, values

        # separate input by user
        user_1_reviews, user_2_reviews = [], []
        for pair in common_ratings:
            user_1_reviews.append(pair[0][1])
            user_2_reviews.append(pair[1][1])

        n_common = len(user_1_reviews)

        num_aspects = len(user_1_reviews[0])

        similarities = []
        for i in range(num_aspects):
            if n_common == 0:
                similarities.append(0.0)
            else:
                rho = pearsonr(
                    np.array([np.float32(x[i]) for x in user_1_reviews]),
                    np.array([np.float32(x[i]) for x in user_2_reviews])
                )[0]

                similarities.append(0.0 if np.isnan(rho) else rho)

        yield (user_1_id, user_2_id), (similarities, n_common)

if __name__ == '__main__':
    UserSimilarities.run()
