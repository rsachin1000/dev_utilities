db = db.getSiblingDB("admin");

db.createUser({
  user: "sachin",
  pwd: "qwerty1234",
  roles: [
    {
      role: "readWrite",
      db: "test_db",
    },
  ],
});
