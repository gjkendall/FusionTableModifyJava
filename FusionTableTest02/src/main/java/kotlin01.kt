//package FusionTableTest02


import com.google.api.services.fusiontables.Fusiontables
import com.google.api.services.fusiontables.Fusiontables.Query.Sql
import com.google.api.services.fusiontables.Fusiontables.Table.Delete
import com.google.api.services.fusiontables.FusiontablesScopes
import com.google.api.services.fusiontables.model.Column
import com.google.api.services.fusiontables.model.Sqlresponse
import com.google.api.services.fusiontables.model.Table
import com.google.api.services.fusiontables.model.TableList


class kt {

    companion object {

        @JvmStatic
        fun kotlintest(i: Int): String {

            print("Enter a Notes string: ")
            val firststr = readLine()!!.split(".",":").first()
            println(firststr)
            println(firststr.length)



            print("Enter two number to add: ")
            val (a, b) = readLine()!!.split(' ')
            println(a.toInt() + b.toInt())


            print("Enter text: ")

            val stringInput = readLine()!!
            println("You entered: $stringInput")
            println("hello $i")
            return stringInput
        }

        @JvmStatic
        fun firstpart(str: String): String {
            return str!!.split(".",":").first()
        }
    }
}



