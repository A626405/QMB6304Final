P <- read.table("Poisson.dat")
colnames(P) <- c("Y","X","X2")
Y <- P$Y
X1 <- P$X
X2 <- P$X2
Xn <- cbind(1,X1,X2)
xprime <- t(Xn)
varcovar <- diag(Y)
Yvec <- as.vector(Y)
Hessian <- xprime %*% varcovar %*% Xn
var <- solve(-1*Hessian)

#Using Learning page 117-118, the var-covar matrix is simply the diagonal of the vector Y.
#In a poisson distribution, the mean is equal to the variance.
#In the no covariate case, Ybar=lambda-hat. When we introduce the link function with a single index we get that Yn = lambda_n = exp[XnB].
#We can calculate the var-covar matrix by using Y since Y = lambda.


log(Yvec)-(Xn%*%B_hat)



beta_hat <- rep(0, ncol(Xn))

#The model converged at B4, I iterated until B6 to make sure.
#Bn is a 3x1 vector, n=3
#Additionally the newton raphson method could have been done with an algorithm.
B1 <- beta_hat - var%*% xprime%*%(Yvec-exp(Xn%*%beta_hat))

B2 <-B1 - var%*% xprime%*%(Yvec-exp(Xn%*%B1))

B3 <- B2 - var%*% xprime%*%(Yvec-exp(Xn%*%B2))

B4 <- B3 - var%*% xprime%*%(Yvec-exp(Xn%*%B3))

B5 <- B4 - var%*% xprime%*%(Yvec-exp(Xn%*%B4))

B6 <- B5 - var%*% xprime%*%(Yvec-exp(Xn%*%B5))
beta_hat_newton <- B6

#Using predict() is the same as using the link function with a single index.
model1<-glm(Y~Xn, family = poisson(link = "log"))
m1_predict<- predict(model1,type = "response")
lambda_n <-exp(Xn%*%B6)

#The LR test statistic is simply the Null Deviance - Residual Deviance.
#Null Deviance represents the model with only the intercept and no covariates.
#Residual Deviance represents the full model.

model2 <- glm(Y~1,family = poisson(link="log"))
m2_predict<- predict(model2,type = "response")

Null <- model1$null.deviance
Full <-model1$deviance
LR_stat <- Null-Full

#The LR test statistic is 32.439
print(LR_stat)

#a = predicted values of the Full model
#a2 = predicted values of the Intercept-only model

#Same value as the LR_stat above
2*sum(m1_predict*log(m1_predict/m2_predict))

#The sum of the Full model is equal to the sum of the intercept only model.
sum(m1_predict)
sum(m2_predict)

#Evaluating the LR test statistic follows a chi squared distribution
#Calculate the difference in DF of the Null-Residual deviance
DF_Diff <- model1$df.null - model1$df.residual
p_value <- pchisq(LR_stat, DF_Diff, lower.tail = FALSE)

print("H0: Bhat_Full = Bhat_Restricted")
print("Ha: Bhat_Full != Bhat_Restricted")

result <- ifelse(p_value < 0.05,
                 "Reject the null hypothesis: The Full model provides a significantly better fit.",
                 "Fail to reject the null hypothesis: The simpler model is sufficient.")
print(result)

beta_hat_model <- coefficients(model1)
