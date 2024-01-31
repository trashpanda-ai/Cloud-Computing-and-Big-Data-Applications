import numpy as np


class KMeans:
    
    def __init__(self, nb_clusters = 2, max_iter=100, init='random', ninit=10):
        self.nb_clusters = nb_clusters
        self.max_iter = max_iter
        self.init = init
        self.ninit = ninit
        self.centroids = None
        self.z = None
        
    def fit(self, X):
        """
        Estimates parameters for the classifier
        
        Args:
            X (array<m,n>): a matrix of floats with
                m rows (#samples) and n columns (#features)
        """
        X = np.asarray(X)
        best_inertia = float('inf')  # Initialize with a high value
        best_centroids = None
        if self.init == 'kmeans++':
            self.centroids = self.initialize_kmeans_pp(X)
        else:
            self.centroids = X[np.random.choice(X.shape[0], self.nb_clusters, replace=False)]
        for _ in range(self.ninit):
            # Initialize centroids with a new random seed for each run
            np.random.seed()  # Use a random seed
            if self.init == 'kmeans++':
                centroids = self.initialize_kmeans_pp(X)
            else:
                centroids = X[np.random.choice(X.shape[0], self.nb_clusters, replace=False)]

            for i in range(self.max_iter):
                dist = cross_euclidean_distance(X, centroids)
                self.z = np.argmin(dist, axis=1)
                new_centroids = np.array([X[self.z == k].mean(axis=0) for k in range(self.nb_clusters)])

                # Check for convergence by comparing centroids
                if np.all(self.centroids == new_centroids):
                    break

                centroids = new_centroids

            # Calculate the inertia (sum of squared distances) for this run
            inertia = np.sum(np.min(dist, axis=1))

            # Check if this run has a lower inertia (better) than the best so far
            if inertia < best_inertia:
                best_inertia = inertia
                best_centroids = centroids

        self.centroids = best_centroids
        return self
    
    def predict(self, X):
        """
        Generates predictions
        
        Note: should be called after .fit()
        
        Args:
            X (array<m,n>): a matrix of floats with 
                m rows (#samples) and n columns (#features)
            
        Returns:
            A length m integer array with cluster assignments
            for each point. E.g., if X is a 10xn matrix and 
            there are 3 clusters, then a possible assignment
            could be: array([2, 0, 0, 1, 2, 1, 1, 0, 2, 2])
        """
        X = np.asarray(X)
        distances = cross_euclidean_distance(X, self.centroids)
        cluster_assignments = np.argmin(distances, axis=-1)
        return cluster_assignments
    
    def get_centroids(self):
        """
        Returns the centroids found by the K-mean algorithm
        
        Example with m centroids in an n-dimensional space:
        >>> model.get_centroids()
        numpy.array([
            [x1_1, x1_2, ..., x1_n],
            [x2_1, x2_2, ..., x2_n],
                    .
                    .
                    .
            [xm_1, xm_2, ..., xm_n]
        ])
        """
        return self.centroids
    def initialize_kmeans_pp(self, X):
        centroids = np.empty((self.nb_clusters, X.shape[1]))
        centroids[0] = X[np.random.choice(X.shape[0])]

        for i in range(1, self.nb_clusters):
            dist = cross_euclidean_distance(X, centroids[:i])
            min_dist = np.min(dist, axis=1)
            prob = min_dist / np.sum(min_dist)

            # Use numpy's random.choice with probabilities for efficiency
            next_centroid_idx = np.random.choice(X.shape[0], p=prob)
            centroids[i] = X[next_centroid_idx]

        return centroids
    
    
    
    
# --- Some utility functions

def euclidean_distance(x, y):
    """
    Computes euclidean distance between two sets of points 
    
    Note: by passing "y=0.0", it will compute the euclidean norm
    
    Args:
        x, y (array<...,n>): float tensors with pairs of 
            n-dimensional points 
            
    Returns:
        A float array of shape <...> with the pairwise distances
        of each x and y point
    """
    return np.linalg.norm(x - y, ord=2, axis=-1)

def cross_euclidean_distance(x, y=None):
    """
    
    
    """
    y = x if y is None else y 
    assert len(x.shape) >= 2
    assert len(y.shape) >= 2
    return euclidean_distance(x[..., :, None, :], y[..., None, :, :])


def euclidean_distortion(X, z):
    """
    Computes the Euclidean K-means distortion
    
    Args:
        X (array<m,n>): m x n float matrix with datapoints 
        z (array<m>): m-length integer vector of cluster assignments
    
    Returns:
        A scalar float with the raw distortion measure 
    """
    X, z = np.asarray(X), np.asarray(z)
    assert len(X.shape) == 2
    assert len(z.shape) == 1
    assert X.shape[0] == z.shape[0]
    
    distortion = 0.0
    clusters = np.unique(z)
    for c in clusters:
        Xc = X[z == c]
        mu = Xc.mean(axis=0)
        distortion += ((Xc - mu) ** 2).sum()  # Compute distortion for this cluster
    
    return distortion


def euclidean_silhouette(X, z):
    """
    Computes the average Silhouette Coefficient with euclidean distance 
    
    More info:
        - https://www.sciencedirect.com/science/article/pii/0377042787901257
        - https://en.wikipedia.org/wiki/Silhouette_(clustering)
    
    Args:
        X (array<m,n>): m x n float matrix with datapoints 
        z (array<m>): m-length integer vector of cluster assignments
    
    Returns:
        A scalar float with the silhouette score
    """
    X, z = np.asarray(X), np.asarray(z)
    assert len(X.shape) == 2
    assert len(z.shape) == 1
    assert X.shape[0] == z.shape[0]
    
    # Compute average distances from each x to all other clusters
    clusters = np.unique(z)
    D = np.zeros((len(X), len(clusters)))
    for i, ca in enumerate(clusters):
        for j, cb in enumerate(clusters):
            in_cluster_a = z == ca
            in_cluster_b = z == cb
            d = cross_euclidean_distance(X[in_cluster_a], X[in_cluster_b])
            div = d.shape[1] - int(i == j)
            D[in_cluster_a, j] = d.sum(axis=1) / np.clip(div, 1, None)
    
    # Intra distance 
    a = D[np.arange(len(X)), z]
    # Smallest inter distance 
    inf_mask = np.where(z[:, None] == clusters[None], np.inf, 0)
    b = (D + inf_mask).min(axis=1)
    
    return np.mean((b - a) / np.maximum(a, b))
  