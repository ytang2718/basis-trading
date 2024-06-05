import math
import numpy as np

class RiskLimit(object):
    """
    Unsigned, indicative of the maximum / minimum risk that can be taken.
    """
    def __init__(
            self,
            min_delta=np.inf,
            max_delta=np.inf,
            min_gamma=np.inf,
            max_gamma=np.inf,
            min_theta=np.inf, 
            max_theta=np.inf, 
            min_vega=np.inf, 
            max_vega=np.inf, 
            min_rho=np.inf, 
            max_rho=np.inf
            ):
        self.min_delta = min_delta
        self.max_delta = max_delta
        self.min_gamma = min_gamma
        self.max_gamma = max_gamma
        self.min_theta = min_theta
        self.max_theta = max_theta
        self.min_vega = min_vega
        self.max_vega = max_vega
        self.min_rho = min_rho
        self.max_rho = max_rho

    def __str__(self):
        return f"Risk-Limit: d: ({self.min_delta}, {self.max_delta}), g: ({self.min_gamma}, {self.max_gamma}), t: ({self.min_theta}, {self.max_theta}), v: ({self.min_vega}, {self.max_vega}), r: ({self.min_rho}, {self.max_rho})"

    def __repr__(self):
        return f"Risk-Limit: d: ({self.min_delta}, {self.max_delta}), g: ({self.min_gamma}, {self.max_gamma}), t: ({self.min_theta}, {self.max_theta}), v: ({self.min_vega}, {self.max_vega}), r: ({self.min_rho}, {self.max_rho})"
        
class Risk(object):
    """
    Risk object encapsulate Greeks. All Greeks are signed.
    """
    def __init__(self, delta=0, gamma=0, theta=0, vega=0, rho=0):
        self.delta = delta
        self.gamma = gamma
        self.theta = theta
        self.vega = vega
        self.rho = rho

    def add(self, other):
        """
        Add another Risk object to this Risk object. Not an in-place operation.
        """
        return Risk(
            delta=self.delta+other.delta,
            gamma=self.gamma+other.gamma,
            theta=self.theta+other.theta,
            vega=self.vega+other.vega, 
            rho=self.rho+other.rho
        )

    def multiply(self, factor:float):
        """
        Multiply all risk measures by a factor. Not an in-place operation.
        """
        return Risk(
        delta=self.delta*factor,
        gamma=self.gamma*factor,
        theta=self.theta*factor,
        vega=self.vega*factor,
        rho=self.rho*factor
        )

    def is_within_risk_limits(self, risk_limit: RiskLimit):
        """
        Returns True if this Risk object is bounded by the given RiskLimit object.
        """
        return self.is_within_delta_limit(risk_limit) and \
                (risk_limit.min_gamma <= self.gamma <= risk_limit.max_gamma) and \
                (risk_limit.min_theta <= self.theta <= risk_limit.max_theta) and \
                (risk_limit.min_vega <= self.vega <= risk_limit.max_vega) and \
                (risk_limit.min_rho <= self.rho <= risk_limit.max_rho)
    
    def is_within_delta_limit(self, delta_limit: RiskLimit):
        """
        Returns True if this Risk object is bounded by the given RiskLimit object.
        """
        return (delta_limit.min_delta <= self.delta <= delta_limit.max_delta)

    def __str__(self):
        return f"Risk: d: {self.delta}, g: {self.gamma}, t: {self.theta}, b: {self.vega}, r: {self.rho}"

    def __repr__(self):
        return f"Risk: d: {self.delta}, g: {self.gamma}, t: {self.theta}, b: {self.vega}, r: {self.rho}"
    

if __name__ == "__main__":
    risk1 = Risk(delta=100, gamma=10, theta=-5, vega=20, rho=15)
    print("Risk 1:", risk1)
    risk2 = Risk(delta=50, gamma=5, theta=-2, vega=10, rho=7)
    print("Risk 2:", risk2)
    risk_limit = RiskLimit(min_delta=-100, max_delta=100, min_gamma=-10, max_gamma=10, min_theta=-5, max_theta=5, min_vega=-20, max_vega=20, min_rho=-15, max_rho=15)
    print("Risk limit:", risk_limit)
    print("Is risk1 under risk_limit?", risk1.is_within_risk_limits(risk_limit))
    # Adding and subtracting risks
    risk_sum = risk1.add(risk2)
    print("Is risk1 + risk2 under delta risk_limit?", risk_sum.is_within_delta_limit(risk_limit))
    # Multiplying risk by a factor
    risk_product = risk1.multiply(5)
    print("Risk 1 * 5:", risk_product)
    # Comparing risks
    print("Is 5x risk1 under risk_limit?", risk_product.is_within_delta_limit(risk_limit))
