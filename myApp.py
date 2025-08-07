from flask import Flask, request, jsonify
from pymongo import MongoClient


app = Flask(__name__)


# #connect to your Mongo DB database
def dockerMongoDB():
    try:
        client = MongoClient(
            host='test_mongodb',
            port=27017,
            username='root',
            password='pass',
            authSource="admin"
        )
        db = client.users
        records = db.register
        counters = db.counters
        print("working till here")
        # Initialize counters if not already set
        if not counters.find_one({"_id": "user_id"}):
            counters.insert_one({"_id": "user_id", "seq": 0})
            print("Initialized counters collection.")

        return records, counters
    except Exception as e:
        print(f"Failed to connect to Docker MongoDB: {e}")
        return None, None

# Initialize both collections
records, counters = dockerMongoDB()

# GET all users
@app.route("/getAllUsers", methods=["GET"])
def get_users():
    print("I'm inside this meho")
    users = list(records.find({}, {"_id": 0}))  # Exclude the _id field
    print(f"Fetched users: {users}") 
    return jsonify(users), 200



@app.route("/users/<variable1>", methods=["GET"])
def get_user_by_username(variable1):
    user = records.find_one({"name": variable1}, {"_id": 0})
    if(user==None):
        user = records.find_one({"email": variable1}, {"_id": 0})
    
    if user:
        return jsonify(user), 200
    return jsonify({"error": "User not found"}), 404


# POST a new user
# POST a new user
@app.route("/users", methods=["POST"])
def create_user():
    if counters is None or records is None:
        return jsonify({"error": "Database connection not established"}), 500

    user_data = request.json
    required_fields = ["name", "email", "phone", "designation"]

    for field in required_fields:
        if not user_data.get(field):
            return jsonify({"error": f"{field} is required"}), 400

    # Get the next sequence ID from counters
    next_id = counters.find_one_and_update(
        {"_id": "user_id"},
        {"$inc": {"seq": 1}},
        return_document=True
    )["seq"]

    # Add the auto-incrementing ID to the user data
    user_data["_id"] = next_id

    # Insert the new user
    records.insert_one(user_data)
    return jsonify({"message": "User created successfully", "id": next_id}), 201


# PUT to update user details
@app.route("/users/<name>", methods=["PUT"])
def update_user(name):
    updated_data = request.json
    result = records.update_one({"name": name}, {"$set": updated_data})

    if result.matched_count == 0:
        return jsonify({"error": "User not found"}), 404

    return jsonify({"message": "User updated successfully"}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)

