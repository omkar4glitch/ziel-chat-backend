export default async function handler(req, res) {
  const { data } = req.query;
  
  if (!data) {
    return res.status(400).send('Missing data parameter');
  }
  
  try {
    const buffer = Buffer.from(data, 'base64');
    
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="Financial_Analysis_${Date.now()}.xlsx"`);
    res.setHeader('Content-Length', buffer.length);
    
    return res.status(200).send(buffer);
  } catch (err) {
    return res.status(500).send('Error generating download');
  }
}
