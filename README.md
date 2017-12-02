# Hadoop
Business.csv file contains the following columns
"business_id"::"full_address"::"categories"

review.csv file contains the following columns
"review_id"::"user_id"::"business_id"::"stars"
'review_id': (a unique identifier for the review)
'user_id': (the identifier of the reviewed business),
'business_id': (the identifier of the authoring user),
'stars': (star rating, integer 1-5), the rating given by the user to a business


part i:
List the business_id, full address and categories of the Top 10 businesses using the
average ratings with reduce side join and job chaining technique.

part ii: 
List the 'user id' and 'rating' of users that reviewed businesses located in “Palo
Alto” with In Memory Join.