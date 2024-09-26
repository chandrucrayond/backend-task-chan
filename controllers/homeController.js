
exports.getHomeData = (req, res) => {
    res.status(200).json({ message: 'You\'re in a home page' });
};


exports.getIdData = (req, res) => {
    const id = req.params.id;  // Get the 'id' parameter from the request

    res.status(200).json({ message: `You\'re in a home page for the book id : ${id}` });
};