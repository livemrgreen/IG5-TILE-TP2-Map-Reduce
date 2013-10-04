// ********************* Questions 1 a 3 **********************

// Creation des documents

var employe0 = {
	name: "employe0",
	age: 15,
	ville: "Montpellier"
};
var employe1 = {
	name: "employe1",
	age: 16,
	ville: "Montpellier"
};
var employe2 = {
	name: "employe2",
	age: 17,
	ville: "Montpellier"
};
var employe3 = {
	name: "employe3",
	age: 18,
	ville: "Montpellier"
};
var employe4 = {
	name: "employe4",
	age: 19,
	ville: "Montpellier"
};
var employe5 = {
	name: "employe5",
	age: 20,
	ville: "Paris"
};
var employe6 = {
	name: "employe6",
	age: 21,
	ville: "Paris"
};
var employe7 = {
	name: "employe7",
	age: 22,
	ville: "Paris"
};
var employe8 = {
	name: "employe8",
	age: 23,
	ville: "Paris"
};
var employe9 = {
	name: "employe9",
	age: 24,
	ville: "Paris"
};
var employe10 = {
	name: "employe10",
	age: 25,
	ville: "San Diego"
};
var employe11 = {
	name: "employe11",
	age: 26,
	ville: "San Diego"
};
var employe12 = {
	name: "employe12",
	age: 27,
	ville: "San Diego"
};
var employe13 = {
	name: "employe13",
	age: 28,
	ville: "San Diego"
};
var employe14 = {
	name: "employe14",
	age: 29,
	ville: "San Diego"
};

// enregistrement des documents
db.emp.save(employe0);
db.emp.save(employe1);
db.emp.save(employe2);
db.emp.save(employe3);
db.emp.save(employe4);
db.emp.save(employe5);
db.emp.save(employe6);
db.emp.save(employe7);
db.emp.save(employe8);
db.emp.save(employe9);
db.emp.save(employe10);
db.emp.save(employe11);
db.emp.save(employe12);
db.emp.save(employe13);
db.emp.save(employe14);

// simulation de la requete :
//		select emp.name, emp.age
//		from emp
//		group by ville

db.emp.mapReduce(
	function() {
		emit(this.ville, this.name);
	},

	function (key, values) {
		return Array.sum(values)
	},

	{
		query: {},
		out: "ville_totals"
	}
);
