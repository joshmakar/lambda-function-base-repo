console.log('Loading function');

exports.handler = async (event, context) => {
    console.log('This code is from github!');
    console.log('event =', JSON.stringify(event, null, 2));
    console.log('context =', JSON.stringify(context, null, 2));
    return 'Done!';
    // throw new Error('Something went wrong');
};
