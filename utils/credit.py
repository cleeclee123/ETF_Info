import numpy as np
from scipy.optimize import newton

from calcs.utils import Bond

class Credit(Bond):
    _benchmark_data = []
    
    def __init__(self, benchmark_data):
        self._benchmark_data = benchmark_data
    
    """
    Usage:
    Assume the bond has a face amount of $1,000, a coupon yield of 5%, a market value of $950, and a maturity of 10 years.
    Assume the annual spot rates for each year are given in the list spot_rates (e.g., [0.02, 0.025, 0.03, ..., 0.05]).
    z_spread = Bond.calculate_z_spread(1000, 0.05, 950, 10, spot_rates)
    """    
    def calculate_z_spread(face_amount, coupon_yield, market_value, T, spot_rates, n=1):
        def present_value(z_spread):
            discount_factors = [(1 + (spot_rate + z_spread) / n) ** (-n * t) for t, spot_rate in enumerate(spot_rates, 1)]
            coupon_payment = face_amount * coupon_yield / n
            pv_coupon_payments = np.sum(coupon_payment * np.array(discount_factors))
            pv_face_value = face_amount * discount_factors[-1]
            return pv_coupon_payments + pv_face_value - market_value

        try:
            z_spread = newton(present_value, x0=0.01)  # Initial guess: 1%
            return z_spread
        except Exception as e:
            print(f"Failed to converge. Error: {e}")
            return None