// Populate the State documents
db.state.insert({_id:'A',short_name:'ACT'});
db.state.insert({_id:'N',short_name:'NSW'});
db.state.insert({_id:'O',short_name:'NT'});
db.state.insert({_id:'Q',short_name:'QLD'});
db.state.insert({_id:'S',short_name:'SA'});
db.state.insert({_id:'T',short_name:'TAS'});
db.state.insert({_id:'V',short_name:'VIC'});
db.state.insert({_id:'W',short_name:'WA'});

// Populate the Status documents
db.status.insert({_id:'D',name:'Deregistered'});
db.status.insert({_id:'R',name:'Registered'});
