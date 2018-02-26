import numpy as np

from .sparse_dot_topn import sparse_dot_topn


def matrix_dot_limit(A, B, ntop,
                     threshold=0., start_row=0, upper_triangular=False):
    """Calculate dot product of sparse matrices

    This function uses a C++ wrapped in Cython implementation. It
    performs A x B and returns the row indices, column indices and
    value if value > `threshold` with a maximum of number of
    `ntop` matches.

    Since the function is optimized for speed it does not perform any
    type of checks on the input (like matrix dimensions).

    Args:
        A (Scipy csr Matrix): Left matrix of dimensions M x K
        B (Scipy csr Matrix): Right matrix of dimensions K x N
        ntop (int): Maximum number of matches to return
        threshold (float): Keep resulting values bigger then this value.
            [Default 0.0]
        start_row (int): First row index of matrix A. Useful when
            performing dot operation in chunks of A. [Default 0]
        upper_triangular (bool): If to return only the upper triangular
            matrix. This is useful when the task is deduplication.
            The upper triangular matrix is defined from the coordenate
            (start_row, start_row) in the resulting matrix.

    Returns:
        Generator[(int, int, float)]: Generator of tuples with
            (row_index, column_index, value).
    """
    B = B.tocsr()

    M = A.shape[0]
    N = B.shape[1]

    idx_dtype = np.int32

    if upper_triangular:
        # massive memory reduction
        # max number of possible non-zero element
        nnz_max = min(int(M * (2 * (N - start_row) - M - 1) / 2), M * ntop)
    else:
        nnz_max = M * ntop

    # arrays will be returned by reference
    rows = np.empty(nnz_max, dtype=idx_dtype)
    cols = np.empty(nnz_max, dtype=idx_dtype)
    data = np.empty(nnz_max, dtype=A.dtype)

    # C++ wrapped with Cython implementation
    # number of found non-zero entries in the upper triangular matrix
    # I'll use this value to slice the returning numpy array
    nnz = sparse_dot_topn(
        M, N,
        np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        ntop,
        threshold,
        rows, cols, data, start_row, int(upper_triangular))

    return ((int(i), int(j), float(v)) for i, j, v in
            zip(rows[:nnz], cols[:nnz], data[:nnz]))
