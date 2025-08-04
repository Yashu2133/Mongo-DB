// Create database
use zen_student_camp;

// Create collections
db.createCollection("students");
db.createCollection("mentors");
db.createCollection("codekata_progress");
db.createCollection("attendance_logs");
db.createCollection("topic_sessions");
db.createCollection("task_submissions");
db.createCollection("drive_events");

// Insert students
db.students.insertMany([
  {
    _id: ObjectId("60000001e61271ef96b8a001"),
    name: "Vaishak",
    email: "vaishakk@zenmail.com",
    mentor_id: ObjectId("7000a20fe61271ef96b8e700")
  },
  {
    _id: ObjectId("60000002e61271ef96b8a002"),
    name: "Sneha",
    email: "sneha@zenmail.com",
    mentor_id: ObjectId("7000a20fe61271ef96b8e700")
  },
  {
    _id: ObjectId("60000003e61271ef96b8a003"),
    name: "Raswanth",
    email: "raswanth@zenmail.com",
    mentor_id: ObjectId("7000a20fe61271ef96b8e701")
  }
]);

// Insert mentors
db.mentors.insertMany([
  {
    _id: ObjectId("7000a20fe61271ef96b8e700"),
    name: "Raghu",
    email: "raghu@zenclass.com",
    mentees_total: 20
  },
  {
    _id: ObjectId("7000a20fe61271ef96b8e701"),
    name: "Yasvant",
    email: "yasvant@zenclass.com",
    mentees_total: 10
  }
]);

// Insert codekata progress
db.codekata_progress.insertMany([
  { user_id: ObjectId('60000001e61271ef96b8a001'), problems_solved: 130 },
  { user_id: ObjectId('60000002e61271ef96b8a002'), problems_solved: 90 },
  { user_id: ObjectId('60000003e61271ef96b8a003'), problems_solved: 60 }
]);

// Insert attendance

db.attendance_logs.insertMany([
  {
    user_id: ObjectId('60000001e61271ef96b8a001'),
    date: '2020-10-20',
    status: 'present'
  },
  {
    user_id: ObjectId('60000002e61271ef96b8a002'),
    date: '2020-10-22',
    status: 'absent'
  },
  {
    user_id: ObjectId('60000003e61271ef96b8a003'),
    date: '2020-10-22',
    status: 'absent'
  }
]);

// Insert topic sessions
db.topic_sessions.insertMany([
  { _id: 101, title: "ES6 Features", date: "2020-10-12" },
  { _id: 102, title: "MongoDB Basics", date: "2020-10-22" },
  { _id: 103, title: "Express Routing", date: "2020-11-03" }
]);

// Insert tasks
db.task_submissions.insertMany([
  {
    task_title: "ES6 Practice",
    task_date: "2020-10-12",
    topic_id: 101,
    user_id: ObjectId("60000001e61271ef96b8a001"),
    submitted: true
  },
  {
    task_title: "Mongo Aggregation Task",
    task_date: "2020-10-22",
    topic_id: 102,
    user_id: ObjectId("60000002e61271ef96b8a002"),
    submitted: false
  },
  {
    task_title: "Routing Basics",
    task_date: "2020-11-03",
    topic_id: 103,
    user_id: ObjectId("60000003e61271ef96b8a003"),
    submitted: true
  }
]);

// Insert drive events
db.drive_events.insertMany([
  {
    company: "Infosys",
    date: "2020-10-18",
    participants: [
      ObjectId("60000001e61271ef96b8a001"),
      ObjectId("60000003e61271ef96b8a003")
    ]
  },
  {
    company: "Wipro",
    date: "2020-10-26",
    participants: [
      ObjectId("60000002e61271ef96b8a002"),
      ObjectId("60000003e61271ef96b8a003")
    ]
  },
  {
    company: "TCS",
    date: "2020-11-01",
    participants: [
      ObjectId("60000001e61271ef96b8a001"),
      ObjectId("60000002e61271ef96b8a002")
    ]
  }
]);

//  1. Find all the topics and tasks which are taught in the month of October

db.topic_sessions.aggregate([
  {
    $match: {
      date: { $gte: "2020-10-01",
              $lte: "2020-10-31" }
    }
  },
  {
    $lookup: {
      from: "task_submissions",
      localField: "_id",
      foreignField: "topic_id",
      as: "related_tasks"
    }
  }
]);


//  2. Find all the company drives which appeared between 15-Oct-2020 and 31-Oct-2020

db.drive_events.find({
  date: { $gte: "2020-10-15", 
          $lte: "2020-10-31" }
});


//  3. Find all the company drives and students who appeared for the placement

db.drive_events.aggregate([
  {
    $lookup: {
      from: "students",
      localField: "participants",
      foreignField: "_id",
      as: "students_attended"
    }
  }
]);

//  4. Find the number of problems solved by each user in code challenges

db.codekata_progress.aggregate([
  {
    $lookup: {
      from: "students",
      localField: "user_id",
      foreignField: "_id",
      as: "student_info"
    }
  },
  {
    $project: {
      _id: 0,
      student_name: { $arrayElemAt: ["$student_info.name", 0] },
      problems_solved: 1
    }
  }
]);


//  5. Find all the mentors who have more than 15 mentees

db.mentors.find({ mentees_total: { $gt: 15 }});


//  6. Find number of users who are absent and did not submit tasks between 15-Oct-2020 and 31-Oct-2020

db.attendance_logs.aggregate([
  {
    $match: {
      date: { $gte: "2020-10-15", $lte: "2020-10-31" },
      status: "absent"
    }
  },
  {
    $lookup: {
      from: "task_submissions",
      let: { uid: "$user_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$user_id", "$$uid"] },
                { $eq: ["$submitted", false] },
                { $gte: ["$task_date", "2020-10-15"] },
                { $lte: ["$task_date", "2020-10-31"] }
              ]
            }
          }
        }
      ],
      as: "unsubmitted_tasks"
    }
  },
  {
    $match: {
      unsubmitted_tasks: { $ne: [] }
    }
  },
  {
    $group: {
      _id: "$user_id"
    }
  },
  {
    $count: "absent_and_unsubmitted_users"
  }
]);


