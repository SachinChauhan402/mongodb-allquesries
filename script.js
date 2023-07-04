// Import the required MongoDB packages
const { MongoClient } = require('mongodb');

// Connection URL and database name
const url = 'mongodb://localhost:27017';
const dbName = 'zen-class-program';

// Function to execute the MongoDB queries
async function executeQueries() {
  // Create a new MongoClient
  const client = new MongoClient(url);

  try {
    // Connect to the MongoDB server
    await client.connect();

    // Select the database
    const db = client.db(dbName);

    // Query 1: Find all the topics and tasks taught in the month of October
    const topicsAndTasks = await db.collection('topics').aggregate([
      {
        $lookup: {
          from: 'tasks',
          localField: 'topic_id',
          foreignField: 'topic_id',
          as: 'tasks',
        },
      },
      {
        $unwind: '$tasks',
      },
      {
        $match: {
          'tasks.date': {
            $gte: new Date('2020-10-01'),
            $lte: new Date('2020-10-31'),
          },
        },
      },
      {
        $project: {
          _id: 0,
          topic: '$name',
          task: '$tasks.task_description',
        },
      },
    ]).toArray();

    console.log('Topics and Tasks in October:');
    console.log(topicsAndTasks);

    // Query 2: Find all the company drives that appeared between 15-Oct-2020 and 31-Oct-2020
    const companyDrives = await db.collection('company_drives').find({
      date: {
        $gte: new Date('2020-10-15'),
        $lte: new Date('2020-10-31'),
      },
    }).toArray();

    console.log('Company Drives in October:');
    console.log(companyDrives);

    // Query 3: Find all the company drives and students who appeared for the placement
    const drivesAndStudents = await db.collection('company_drives').aggregate([
      {
        $lookup: {
          from: 'attendance',
          localField: 'date',
          foreignField: 'date',
          as: 'attendance',
        },
      },
      {
        $lookup: {
          from: 'users',
          localField: 'attendance.user_id',
          foreignField: 'user_id',
          as: 'students',
        },
      },
      {
        $match: {
          'students.role': 'student',
        },
      },
      {
        $project: {
          _id: 0,
          company: '$company_name',
          students: '$students.name',
        },
      },
    ]).toArray();

    console.log('Company Drives and Students:');
    console.log(drivesAndStudents);

    // Query 4: Find the number of problems solved by a user in Codekata
    const userProblems = await db.collection('codekata').aggregate([
      {
        $lookup: {
          from: 'users',
          localField: 'user_id',
          foreignField: 'user_id',
          as: 'user',
        },
      },
      {
        $project: {
          _id: 0,
          user: '$user.name',
          problemCount: '$problem_count',
        },
      },
    ]).toArray();

    console.log('User Problems in Codekata:');
    console.log(userProblems);

    // Query 5: Find all the mentors who have a mentee count greater than 15
    const mentors = await db.collection('mentors').find({
      mentee_count: {
        $gt: 15,
      },
    }).toArray();

    console.log('Mentors with More than 15 Mentees:');
    console.log(mentors);

    // Query 6: Find the number of users who were absent and task was not submitted between 15-Oct-2020 and 31-Oct-2020
    const absentUsers = await db.collection('attendance').aggregate([
      {
        $lookup: {
          from: 'tasks',
          localField: 'user_id',
          foreignField: 'user_id',
          as: 'task',
        },
      },
      {
        $match: {
          $and: [
            {
              status: 'absent',
            },
            {
              $or: [
                {
                  'task.date': {
                    $lt: new Date('2020-10-15'),
                  },
                },
                {
                  'task.date': {
                    $gt: new Date('2020-10-31'),
                  },
                },
                {
                  'task.date': {
                    $exists: false,
                  },
                },
              ],
            },
          ],
        },
      },
      {
        $count: 'absentUsersCount',
      },
    ]).toArray();

    console.log('Number of Absent Users with Unsubmitted Tasks:');
    console.log(absentUsers[0].absentUsersCount);
  } catch (error) {
    console.error('An error occurred:', error);
  } finally {
    // Close the MongoDB connection
    await client.close();
  }
}
        console.log(executeQueries())
// Execute the MongoDB queries
// executeQueries();
