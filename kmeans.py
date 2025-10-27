import numpy as np
import pandas as pd

class KMeans(object):

    def __init__(self, k, start_var, end_var, num_observations, data, calcMode):
        """Class constructor for KMeans
        Arguments:
            k {int} -- number of clusters to create from the data
            start_var {int} -- starting index of the variables (columns) to
            consider in creating clusters from the dataset. This is
            useful for excluding some columns for clustering.
            end_var {int} -- ending index of the variables (columns) to
            consider in creating clusters from the dataset. This is
            useful for excluding some columns for clustering.
            num_observations {int} -- total number of observations (rows) in
            the dataset.
            data {DataFrame} -- the dataset to cluster
        """
        np.random.seed(1)
        self.k = k
        self.start_var = start_var
        self.end_var = end_var
        self.num_observations = num_observations
        self.columns = [i for i in data.columns[start_var:end_var]]
        self.centroids = pd.DataFrame(columns=self.columns)
        self.calcMode = calcMode
        self.distancesMean = []

    def getDistancesMean(self):
        return self.distancesMean


    def initialize_centroids(self, data):
        """Returns initial centroids. This function picks a random point from
        the dataset as the first centroid, then iteratively picks points that
        are farthest from the current set of centroids.

        The algorithm for this initialization is as follows:
        1. Randomly select the first centroid from the data points in the
        dataset.
        2. For each data point, compute its distance from each centroid in the
        current set of centroids. For each distance computed from each
        centroid, retain only the shortest distance for each data point. In
        other words, we are computing the distance of each data point from
        the nearest centroid.
        3. Select the data point with the maximum distance from the nearest
        centroid as the next centroid.
        4. Repeat steps 2 and 3 until we have k number of centroids.

        Arguments:
            data {DataFrame} -- dataset to cluster
        Returns:
            DataFrame -- contains the values of the initial location of the
            centroids.
        """

        index = np.random.randint(low=0, high=self.num_observations)
        
        point = data.iloc[index, self.start_var:self.end_var]
        point = point.set_axis(self.centroids.columns).to_frame().T
        self.centroids = pd.concat([self.centroids, point], ignore_index=True)
        sliced_data = data.iloc[:, self.start_var:self.end_var]
        

        for i in range(1, self.k):


            distances = pd.DataFrame()
            

            for j in range(len(self.centroids)):
                if (self.calcMode == 2):
                    distance = self.get_cosine_distance(sliced_data, self.centroids.iloc[j])
                else:
                    distance = self.get_euclidean_distance(sliced_data, self.centroids.iloc[j])             
                
                

                distances[j] = distance
                        


            min_distances = distances.min(axis=1)
            index = min_distances.idxmax()
                

            point = data.iloc[index, self.start_var:self.end_var]
            point = point.set_axis(self.centroids.columns).to_frame().T
            self.centroids = pd.concat([self.centroids, point], ignore_index=True)

        return self.centroids

        
    def get_cosine_distance(self, point1, point2):
        """Returns the Euclidean distance between two data points. These
        data points can be represented as 2 Series objects. This function can
        also compute the Euclidean distance between a list of data points
        (represented as a DataFrame) and a single data point (represented as
        a Series), using broadcasting.

        The Euclidean distance can be computed by getting the square root of
        the sum of the squared difference between each variable of each data
        point.

        For the arguments point1 and point2, you can only pass these
        combinations of data types:
        - Series and Series -- returns np.float64
        - DataFrame and Series -- returns pd.Series

        For a DataFrame and a Series, if the shape of the DataFrame is
        (3, 2), the shape of the Series should be (2,) to enable broadcasting.
        This operation will result to a Series of shape (3,)

        Arguments:
            point1 {Series or DataFrame} - data point
            point2 {Series or DataFrame} - data point
        Returns:
            np.float64 or pd.Series -- contains the Euclidean distance
            between the data points.
        """
    
        point1 = point1.astype(float)
        point2 = point2.astype(float)
    
        dot_product = point1.dot(point2)
    
        norm_point1 = np.linalg.norm(point1)
        norm_point2 = np.linalg.norm(point2)

        if norm_point1 == 0 or norm_point2 == 0:
            return 1.0  
    
        cosine_similarity = dot_product / (norm_point1 * norm_point2)
        cosine_distance = 1 - cosine_similarity
    
        return cosine_distance

        

    def get_euclidean_distance(self, point1, point2):
        """Returns the Euclidean distance between two data points. These
        data points can be represented as 2 Series objects. This function can
        also compute the Euclidean distance between a list of data points
        (represented as a DataFrame) and a single data point (represented as
        a Series), using broadcasting.

        The Euclidean distance can be computed by getting the square root of
        the sum of the squared difference between each variable of each data
        point.

        For the arguments point1 and point2, you can only pass these
        combinations of data types:
        - Series and Series -- returns np.float64
        - DataFrame and Series -- returns pd.Series

        For a DataFrame and a Series, if the shape of the DataFrame is
        (3, 2), the shape of the Series should be (2,) to enable broadcasting.
        This operation will result to a Series of shape (3,)

        Arguments:
            point1 {Series or DataFrame} - data point
            point2 {Series or DataFrame} - data point
        Returns:
            np.float64 or pd.Series -- contains the Euclidean distance
            between the data points.
        """

        sq_sum = (point1-point2)**2
        sq_sum = sq_sum.astype(float)

        if ((len(point1.shape) >= 2) ^ (len(point2.shape) >= 2)) and point1.shape[1] == point2.shape[0]:
            row_sum= sq_sum.sum(axis=1)
            return np.sqrt(row_sum)
        else:
            col_sum = sq_sum.sum()
            return np.sqrt(col_sum)

    def group_observations(self, data):
        """Returns the clusters of each data point in the dataset given
        the current set of centroids. Suppose this function is given 100 data
        points to cluster into 3 groups, the function returns a Series of
        shape (100,), where each value is between 0 to 2.

        Arguments:
            data {DataFrame} -- dataset to cluster
        Returns:
            Series -- represents the cluster of each data point in the dataset.
        """


        distances = pd.DataFrame()
        sliced_data = data.iloc[:, self.start_var:self.end_var]
        for i in range(self.k):
            if(self.calcMode == 2):

                distances[i] = self.get_cosine_distance(sliced_data, self.centroids.iloc[i]) 
            else:
                distances[i] = self.get_euclidean_distance(sliced_data, self.centroids.iloc[i])
       

            

        groups = pd.Series(distances.idxmin(axis=1))



        return groups

    def adjust_centroids(self, data, groups):

        grouped_data = pd.concat([data, groups.rename('group')], axis=1)

        centroids = grouped_data.groupby('group')[self.columns].mean()
        # print(grouped_data)
        # print(f"We have {self.k} but the shape is {centroids.shape}")

        return centroids

    def train(self, data, iters):

        cur_groups = pd.Series(-1, index=[i for i in range(self.num_observations)])
        i = 0
        flag_groups = False
        flag_centroids = False


        while i < iters and not flag_groups and not flag_centroids:


            groups = self.group_observations(data)


 
            centroids = self.adjust_centroids(data, groups)


            flg = (groups == cur_groups).all()

            if flg:
                break

            flg = ((centroids.shape ==  self.centroids.shape) & ((centroids.iloc == self.centroids.iloc)))

            if flg:
                break
            cur_groups = groups
            self.centroids = centroids

            i += 1
        print(f"Finished Clustering at {i} iterations")

            


        return cur_groups

