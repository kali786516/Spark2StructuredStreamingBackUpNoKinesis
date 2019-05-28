
.......................Product.......................


                                      product

                 deposits                                      lending
 checkings                       savings                       creditcards
corecheckings studentcheckings   RewardsSavings

prodcut --> name

deposits -->   val interestRatePerYear:Double ,
               val minimalBalancePerMonth:Int

checkings --> Checkings extends Deposits

corecheckings -->   override val interestRatePerYear:Double = rate,
                    override val minimalBalancePerMonth: Int = bal ,
                    override val name: String = "Core Checking"

StudentChecking -->   override val minimalBalancePerMonth: Int = bal,
                      override val interestRatePerYear: Double = rate,
                      override val name: String = "Student Checking"

Savings -->   val transactionAllowedPerMonth:Int

RewardsSavings -->   override val minimalBalancePerMonth: Int = bal,
                     override val interestRatePerYear: Double = rate,
                     override val name: String = "Reward Savings",
                     override val transactionAllowedPerMonth: Int = trans

Lending  --> extends prodcut

creditcards -->   override val annualFee:Double = fee
                  override val apr:Double = rate
                  override val rewardsPercent:Double = pct
                  override val name:String = "Credit Card"

.......................Customer.......................

firstname,lastname,dob,email

.......................Account........................

Account --> Customer and Product
            private var balance:Int = b
            func deposit
            func withdrawl

.......................Bank........................

Bank -->   val name = n
           val city = c
           val country = co
           val email = e
           val products:Set[entities.Product] = ps
           val customer:Set[Customer] = cs
           val accounts:Set[Account] = as

.......................Bank of Scala (Main Class).......................





